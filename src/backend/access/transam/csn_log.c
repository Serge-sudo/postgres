/*-----------------------------------------------------------------------------
 *
 * csn_log.c
 *		Track commit sequence numbers of finished transactions
 *
 * This module provides SLRU to store CSN for each transaction.  This
 * mapping need to be kept only for xid's greater then oldestXid, but
 * that can require arbitrary large amounts of memory in case of long-lived
 * transactions.  Because of same lifetime and persistancy requirements
 * this module is quite similar to subtrans.c
 *
 * If we switch database from CSN-base snapshot to xid-base snapshot then,
 * nothing wrong. But if we switch xid-base snapshot to CSN-base snapshot
 * it should decide a new xid which begin csn-base check. It can not be
 * oldestActiveXID because of prepared transaction.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csn_log.c
 *
 *-----------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csn_log.h"
#include "access/slru.h"
#include "access/csn_snapshot.h"
#include "access/subtrans.h"
#include "access/xloginsert.h"
#include "access/transam.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "portability/instr_time.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/snapmgr.h"
#include "access/xlog_internal.h"

/*
 * We use csnSnapshotActive to judge if csn snapshot enabled instead of by
 * enable_csn_snapshot, this design is similar to 'track_commit_timestamp'.
 *
 * Because in process of replication if master changes 'enable_csn_snapshot'
 * in a database restart, standby should apply wal record for GUC changed,
 * then it's difficult to notice all backends about that. So they can get
 * the message by 'csnSnapshotActive' which in shared buffer. It will not
 * acquire a lock, so without performance issue.
 * last_max_csn - Record the max csn till now.
 * last_csn_log_wal - for interval we log the assign csn to wal
 * oldestXmin - first sensible Xmin on the first existed page in the CSN Log
 * 
 * HLC fields for Hybrid Logical Clock support:
 * hlc_physical_time - last physical timestamp used in HLC
 * hlc_logical_counter - logical counter for current physical time
 */
typedef struct CSNShared
{
	bool				csnSnapshotActive;
	pg_atomic_uint32	oldestXmin;
	CSN					last_max_csn;
	CSN					last_csn_log_wal;
	/* HLC state */
	pg_atomic_uint64	hlc_physical_time;
	pg_atomic_uint32	hlc_logical_counter;
	volatile slock_t	lock;
} CSNShared;

CSNShared *csnShared;

/*
 * Defines for CSNLog page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLog page numbering also wraps around at
 * 0xFFFFFFFF/CSN_LOG_XACTS_PER_PAGE, and CSNLog segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLog (see CSNLogPagePrecedes).
 */

/* We store the commit CSN for each xid */
#define CSN_LOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CSN))

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CSN_LOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CSN_LOG_XACTS_PER_PAGE)

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CSNLogCtlData;
#define CsnlogCtl (&CSNLogCtlData)

static int	ZeroCSNLogPage(int64 pageno, bool write_xlog);
static void ZeroTruncateCSNLogPage(int64 pageno, bool write_xlog);
static bool CSNLogPagePrecedes(long page1, long page2);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids,
									  TransactionId *subxids,
									  CSN csn, int64 pageno);
static void CSNLogSetCSNInSlot(TransactionId xid, CSN csn, int slotno);

static void WriteCSNXlogRec(TransactionId xid, int nsubxids,
							TransactionId *subxids, CSN csn);
static void WriteZeroCSNPageXlogRec(int64 pageno);
static void WriteTruncateCSNXlogRec(int64 pageno);
static void set_oldest_xmin(TransactionId xid);


/*
 * Number of shared CSNLog buffers.
 */
static Size
CSNLogShmemBuffers(void)
{
	return (NBuffers / 512) > 16 ? 32 : 16;
}

/*
 * Reserve shared memory for CsnlogCtl.
 */
Size
CSNLogShmemSize(void)
{
	return SimpleLruShmemSize(CSNLogShmemBuffers(), 0);
}

/*
 * Initialization of shared memory for CSNLog.
 */
void
CSNLogShmemInit(void)
{
	bool		found;

	CsnlogCtl->PagePrecedes = CSNLogPagePrecedes;
	SimpleLruInit(CsnlogCtl, "CSNLog Ctl", CSNLogShmemBuffers(), 0,
				  "pg_csn", LWTRANCHE_CSN_LOG_BUFFERS,
				  LWTRANCHE_CSN_LOG_SLRU, SYNC_HANDLER_CSN, false);

	csnShared = ShmemInitStruct("CSNlog shared",
									 sizeof(CSNShared),
									 &found);
	if (!found)
	{
		csnShared->csnSnapshotActive = false;
		pg_atomic_init_u32(&csnShared->oldestXmin, InvalidTransactionId);
		csnShared->last_max_csn = InvalidCSN;
		csnShared->last_csn_log_wal = InvalidCSN;
		/* Initialize HLC fields */
		pg_atomic_init_u64(&csnShared->hlc_physical_time, 0);
		pg_atomic_init_u32(&csnShared->hlc_logical_counter, 0);
		SpinLockInit(&csnShared->lock);
	}
}

/*
 * CSNLogSetCSN
 *
 * Record CSN of transaction and its subtransaction tree.
 *
 * xid is a single xid to set status for. This will typically be the top level
 * transactionid for a top level commit or abort. It can also be a
 * subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * AbortedCSN for abort cases.
 */
void
CSNLogSetCSN(TransactionId xid, int nsubxids, TransactionId *subxids, CSN csn,
			 bool write_xlog)
{
	int64 pageno;
	int i = 0;
	int offset = 0;

	Assert(TransactionIdIsValid(xid));

	pageno = TransactionIdToPage(xid);		/* get page of parent */

	if(write_xlog)
		WriteCSNXlogRec(xid, nsubxids, subxids, csn);

	for (;;)
	{
		int64 num_on_page = 0;

		/* Form subtransactions bucket that can be written on the same page */
		while (i < nsubxids && TransactionIdToPage(subxids[i]) == pageno)
		{
			num_on_page++;
			i++;
		}

		CSNLogSetPageStatus(xid,
							num_on_page, subxids + offset,
							csn, pageno);
		if (i >= nsubxids)
			break;

		offset = i;
		pageno = TransactionIdToPage(subxids[offset]);
		xid = InvalidTransactionId;
	}
}

/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CSNLogSetPageStatus(TransactionId xid, int nsubxids, TransactionId *subxids,
					CSN csn, int64 pageno)
{
	int slotno;
	int i;
	LWLock	   *lock;

	lock = SimpleLruGetBankLock(CsnlogCtl, pageno);

	LWLockAcquire(lock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CsnlogCtl, pageno, true, xid);

	/* Subtransactions first, if needed ... */
	for (i = 0; i < nsubxids; i++)
	{
		Assert(CsnlogCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CSNLogSetCSNInSlot(subxids[i], csn, slotno);
	}

	/* ... then the main transaction */
	if (TransactionIdIsValid(xid))
		CSNLogSetCSNInSlot(xid, csn, slotno);

	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(lock);
}

/*
 * Sets the commit status of a single transaction.
 */
static void
CSNLogSetCSNInSlot(TransactionId xid, CSN csn, int slotno)
{
	int entryno = TransactionIdToPgIndex(xid);
	CSN *ptr;

	Assert(LWLockHeldByMe(CSNLogLock));

	ptr = (CSN *) (CsnlogCtl->shared->page_buffer[slotno] +
														entryno * sizeof(CSN));
	*ptr = csn;
}

/*
 * Interrogate the state of a transaction in the log.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionIdGetCSN() in csn_snapshot.c is the
 * intended caller.
 */
CSN
CSNLogGetCSNByXid(TransactionId xid)
{
	int64 pageno = TransactionIdToPage(xid);
	int entryno = TransactionIdToPgIndex(xid);
	int slotno;
	CSN csn;
	LWLock	   *lock;

	lock = SimpleLruGetBankLock(CsnlogCtl, pageno);

	/* lock is acquired by SimpleLruReadPage_ReadOnly */
	slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl, pageno, xid);
	csn = *(CSN *) (CsnlogCtl->shared->page_buffer[slotno] +
														entryno * sizeof(CSN));
	LWLockRelease(lock);

	return csn;
}

/*
 * Initialize (or reinitialize) a page of CSNLog to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCSNLogPage(int64 pageno, bool write_xlog)
{
	Assert(LWLockHeldByMe(CSNLogLock));
	if(write_xlog)
		WriteZeroCSNPageXlogRec(pageno);
	return SimpleLruZeroPage(CsnlogCtl, pageno);
}

static void
ZeroTruncateCSNLogPage(int64 pageno, bool write_xlog)
{
	if(write_xlog)
		WriteTruncateCSNXlogRec(pageno);
	SimpleLruTruncate(CsnlogCtl, pageno);
}

void
ActivateCSNlog(void)
{
	int64				pageno;
	TransactionId	nextXid = InvalidTransactionId;
	TransactionId	oldest_xid = InvalidTransactionId;

	LWLockAcquire(CSNLogLock, LW_EXCLUSIVE);
	if (csnShared->csnSnapshotActive)
	{
		LWLockRelease(CSNLogLock);
		return;
	}
	LWLockRelease(CSNLogLock);

	nextXid = XidFromFullTransactionId(TransamVariables->nextXid);
	pageno = TransactionIdToPage(nextXid);

	/*
	 * Create the current segment file, if necessary.
	 * This means that
	 */
	if (!SimpleLruDoesPhysicalPageExist(CsnlogCtl, pageno))
	{
		int slotno;
		TransactionId curxid = nextXid;
		LWLock	   *lock = SimpleLruGetBankLock(CsnlogCtl, pageno);
		LWLockAcquire(lock, LW_EXCLUSIVE);

		slotno = ZeroCSNLogPage(pageno, false);
		SimpleLruWritePage(CsnlogCtl, slotno);
		LWLockRelease(lock);

		elog(LOG, "Create SLRU page=%ld, slotno=%d for xid %u on a CSN log activation",
			 pageno, slotno, nextXid);

		/*
		 * nextXid isn't first xid on the page. It is the first page in the CSN
		 * log. Set UnclearCSN value into all previous slots on this page.
		 * This xid value can be used as an oldest xid in the CSN log.
		 */
		if (TransactionIdToPgIndex(nextXid) > 0)
		{
			/* Cleaning procedure. Can be optimized. */
			do
			{
				curxid--;
				CSNLogSetCSNInSlot(curxid, UnclearCSN, slotno);
			} while (TransactionIdToPgIndex(curxid) > 0);

			elog(LOG,
				 "Set UnclearCSN values for %d xids in the range [%u,%u]",
				 nextXid - curxid, curxid, nextXid-1);

			/* Oldest XID found on this page */
			oldest_xid = nextXid;
		}
	}

	if (!TransactionIdIsValid(oldest_xid))
	{
		TransactionId curxid;

		elog(LOG, "Search for the oldest xid across previous pages");

		/* Need to scan previous pages for an oldest xid. */
		while (pageno > 0 && SimpleLruDoesPhysicalPageExist(CsnlogCtl, pageno - 1))
			pageno--;

		/* look up for the first clear xid value. */
		curxid = pageno * (TransactionId) CSN_LOG_XACTS_PER_PAGE;
		while(CSNLogGetCSNByXid(curxid) == UnclearCSN)
			curxid++;
		oldest_xid = curxid;
	}

	set_oldest_xmin(oldest_xid);
	LWLockAcquire(CSNLogLock, LW_EXCLUSIVE);
	csnShared->csnSnapshotActive = true;
	LWLockRelease(CSNLogLock);

}

bool
get_csnlog_status(void)
{
	return csnShared->csnSnapshotActive;
}

void
DeactivateCSNlog(void)
{
	csnShared->csnSnapshotActive = false;
	set_oldest_xmin(InvalidTransactionId);
	LWLockAcquire(CSNLogLock, LW_EXCLUSIVE);
	(void) SlruScanDirectory(CsnlogCtl, SlruScanDirCbDeleteAll, NULL);
	LWLockRelease(CSNLogLock);
	elog(LOG, "CSN log has deactivated");
}

void
StartupCSN(void)
{
	ActivateCSNlog();
}

void
CompleteCSNInitialization(void)
{
	/*
	 * If the feature is not enabled, turn it off for good.  This also removes
	 * any leftover data.
	 *
	 * Conversely, we activate the module if the feature is enabled.  This is
	 * necessary for primary and standby as the activation depends on the
	 * control file contents at the beginning of recovery or when a
	 * XLOG_PARAMETER_CHANGE is replayed.
	 */
	if (!enable_csn_snapshot)
		DeactivateCSNlog();
	else
		ActivateCSNlog();
}

void
CSNlogParameterChange(bool newvalue, bool oldvalue)
{
	if (newvalue)
	{
		if (!csnShared->csnSnapshotActive)
			ActivateCSNlog();
	}
	else if (csnShared->csnSnapshotActive)
		DeactivateCSNlog();
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCSNLog(void)
{
	if (!get_csnlog_status())
		return;

	/*
	 * Flush dirty CSNLog pages to disk.
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(true);
	SimpleLruWriteAll(CsnlogCtl, true);
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(true);
}

/*
 * Make sure that CSNLog has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCSNLog(TransactionId newestXact)
{
	int64			pageno;
	LWLock	   *lock;

	if (!get_csnlog_status())
		return;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);
	lock = SimpleLruGetBankLock(CsnlogCtl, pageno);

	LWLockAcquire(lock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCSNLogPage(pageno, !InRecovery);

	LWLockRelease(lock);
}

/*
 * Remove all CSNLog segments before the one holding the passed
 * transaction ID.
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCSNLog(TransactionId oldestXact)
{
	int				cutoffPage;
	TransactionId	oldestXmin;

	/* Can't do truncation because WAL messages isn't allowed during recovery */
	if (RecoveryInProgress() || !get_csnlog_status())
		return;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate. We step
	 * back one transaction to avoid passing a cutoff page that hasn't been
	 * created yet in the rare case that oldestXact would be the first item on
	 * a page and oldestXact == next XID.  In that case, if we didn't subtract
	 * one, we'd trigger SimpleLruTruncate's wraparound detection.
	 */
	TransactionIdRetreat(oldestXact);
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Detect, that we really need to cut CSN log. */
	oldestXmin = pg_atomic_read_u32(&csnShared->oldestXmin);

	if (TransactionIdToPage(oldestXmin) < cutoffPage)
	{
		/* OldestXact is located in the same page as oldestXmin. No actions needed. */
		return;
	}

	/*
	 * Shift oldestXmin to the start of new first page. Use first position
	 * on the page because all transactions on this page is created with enabled
	 * CSN snapshot machinery.
	 */
	pg_atomic_write_u32(&csnShared->oldestXmin,
						oldestXact - TransactionIdToPgIndex(oldestXact));

	SpinLockRelease(&csnShared->lock);
	ZeroTruncateCSNLogPage(cutoffPage, true);
}

/*
 * Decide which of two CSNLog page numbers is "older" for truncation
 * purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CSNLogPagePrecedes(long page1, long page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CSN_LOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CSN_LOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}

void
WriteAssignCSNXlogRec(CSN csn)
{
	Assert(enable_csn_wal && csn <= csnShared->last_csn_log_wal);

	XLogBeginInsert();
	XLogRegisterData((char *) (&csn), sizeof(CSN));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_ASSIGNMENT);
}

static void
WriteCSNXlogRec(TransactionId xid, int nsubxids,
				TransactionId *subxids, CSN csn)
{
	xl_csn_set xlrec;

	if(!enable_csn_wal)
		return;

	xlrec.xtop = xid;
	xlrec.nsubxacts = nsubxids;
	xlrec.csn = csn;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, MinSizeOfCSNSet);
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_SETCSN);
}

/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroCSNPageXlogRec(int64 pageno)
{
	if(!enable_csn_wal)
	{
		return;
	}
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int64));
	(void) XLogInsert(RM_CSNLOG_ID, XLOG_CSN_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 */
static void
WriteTruncateCSNXlogRec(int64 pageno)
{
	if(!enable_csn_wal)
	{
		return;
	}
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int64));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_TRUNCATE);
}


void
csnlog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in csnlog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == XLOG_CSN_ASSIGNMENT)
	{
		CSN csn;

		memcpy(&csn, XLogRecGetData(record), sizeof(CSN));
		/* XXX: Do we really not needed to acquire the lock here? */
		csnShared->last_max_csn = csn;
	}
	else if (info == XLOG_CSN_SETCSN)
	{
		xl_csn_set *xlrec = (xl_csn_set *) XLogRecGetData(record);
		CSNLogSetCSN(xlrec->xtop, xlrec->nsubxacts, xlrec->xsub, xlrec->csn, false);
	}
	else if (info == XLOG_CSN_ZEROPAGE)
	{
		int64			pageno;
		int			slotno;
		LWLock	   *lock;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int64));
		lock = SimpleLruGetBankLock(CsnlogCtl, pageno);
		LWLockAcquire(lock, LW_EXCLUSIVE);
		slotno = ZeroCSNLogPage(pageno, false);
		SimpleLruWritePage(CsnlogCtl, slotno);
		LWLockRelease(lock);
		Assert(!CsnlogCtl->shared->page_dirty[slotno]);

	}
	else if (info == XLOG_CSN_TRUNCATE)
	{
		int64			pageno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int64));
		pg_atomic_write_u64(&CsnlogCtl->shared->latest_page_number,
			pageno);
		ZeroTruncateCSNLogPage(pageno, false);
	}
	else
		elog(PANIC, "csnlog_redo: unknown op code %u", info);
}

/*
 * Entrypoint for sync.c to sync members files.
 */
int
csnsyncfiletag(const FileTag *ftag, char *path)
{
	return SlruSyncFileTag(&CSNLogCtlData, ftag, path);
}

/*
 * GenerateCSN
 *
 * Generate CSN using Hybrid Logical Clock algorithm instead of simple timestamps.
 * This replaces the Clock-SI implementation to avoid Microsoft patent issues.
 * 
 * For compatibility, this function now wraps the HLC implementation.
 */
CSN
GenerateCSN(bool locked, CSN assign)
{
	/* If assign is provided, treat it as a remote HLC to advance beyond */
	return GenerateHLCTimestamp(locked, assign);
}

CSN
GetLastGeneratedCSN(void)
{
	CSN csn;

	SpinLockAcquire(&csnShared->lock);
	csn = csnShared->last_max_csn;
	SpinLockRelease(&csnShared->lock);
	return csn;
}

/*
 * Mostly for debug purposes.
 */
static void
set_oldest_xmin(TransactionId xid)
{
	elog(LOG, "Oldest Xmin for CSN will be changed from %u to %u",
		 pg_atomic_read_u32(&csnShared->oldestXmin), xid);

	pg_atomic_write_u32(&csnShared->oldestXmin, xid);
}

TransactionId
GetOldestXmin(void)
{
	Assert(get_csnlog_status());
	return pg_atomic_read_u32(&csnShared->oldestXmin);
}

/*
 * ========================
 * Hybrid Logical Clock Implementation
 * ========================
 * 
 * This implementation replaces the Clock-SI algorithm to avoid Microsoft patent issues.
 * Based on the HLC algorithm used in CockroachDB for global snapshot isolation.
 * 
 * HLC ensures global ordering of events across distributed nodes while maintaining
 * partial ordering based on causality.
 */

/*
 * HLCLess - compare two HLC timestamps
 * Returns true if hlc1 < hlc2 according to HLC ordering
 */
bool
HLCLess(CSN hlc1, CSN hlc2)
{
	uint64 pt1 = CSN_TO_HLC_PHYSICAL(hlc1);
	uint64 pt2 = CSN_TO_HLC_PHYSICAL(hlc2);
	uint32 l1 = CSN_TO_HLC_LOGICAL(hlc1);
	uint32 l2 = CSN_TO_HLC_LOGICAL(hlc2);

	if (pt1 != pt2)
		return pt1 < pt2;
	return l1 < l2;
}

/*
 * HLCEqual - check if two HLC timestamps are equal
 */
bool
HLCEqual(CSN hlc1, CSN hlc2)
{
	return hlc1 == hlc2;
}

/*
 * GenerateHLCTimestamp - Generate HLC timestamp for local events
 * 
 * This is the main replacement for GenerateCSN using HLC algorithm
 * locked: whether caller already holds the lock
 * remote_hlc: if not InvalidCSN, advance to be greater than this remote HLC
 */
CSN
GenerateHLCTimestamp(bool locked, CSN remote_hlc)
{
	instr_time	current_time;
	uint64		now_ns;
	uint64		pt, remote_pt = 0;
	uint32		l, remote_l = 0;
	CSN			new_hlc;
	CSN			log_csn = InvalidCSN;

	Assert(get_csnlog_status() || csn_snapshot_defer_time > 0);

	/* Get current physical time in nanoseconds */
	INSTR_TIME_SET_CURRENT(current_time);
	now_ns = (uint64) INSTR_TIME_GET_NANOSEC(current_time) + (int64) (csn_time_shift * 1E9);
	
	/* Apply mask to fit in our bit allocation */
	now_ns = now_ns & HLC_PHYSICAL_MASK;

	/* Extract remote HLC components if provided */
	if (remote_hlc != InvalidCSN)
	{
		remote_pt = CSN_TO_HLC_PHYSICAL(remote_hlc);
		remote_l = CSN_TO_HLC_LOGICAL(remote_hlc);
	}

	if (!locked)
		SpinLockAcquire(&csnShared->lock);

	/* Get current HLC state */
	pt = pg_atomic_read_u64(&csnShared->hlc_physical_time);
	l = pg_atomic_read_u32(&csnShared->hlc_logical_counter);

	/* HLC algorithm for local event advancement */
	if (remote_hlc != InvalidCSN)
	{
		/* Advance based on remote HLC (receiving a message) */
		pt = now_ns > remote_pt ? now_ns : remote_pt;
		if (pt == remote_pt)
		{
			/* Same physical time as remote, increment logical counter */
			l = (l > remote_l ? l : remote_l) + 1;
		}
		else
		{
			/* Different physical time, reset logical counter */
			l = 0;
		}
	}
	else
	{
		/* Local event advancement */
		if (now_ns > pt)
		{
			/* Physical time advanced, reset logical counter */
			pt = now_ns;
			l = 0;
		}
		else
		{
			/* Same or earlier physical time, increment logical counter */
			l++;
		}
	}

	/* Ensure logical counter doesn't overflow */
	if (l > HLC_LOGICAL_MASK)
	{
		/* If logical counter overflows, advance physical time */
		pt++;
		l = 0;
	}

	/* Store new HLC state */
	pg_atomic_write_u64(&csnShared->hlc_physical_time, pt);
	pg_atomic_write_u32(&csnShared->hlc_logical_counter, l);

	/* Pack into CSN format */
	new_hlc = HLC_TO_CSN(((HybridLogicalClock){pt, l}));

	/* Update last_max_csn for compatibility */
	if (new_hlc > csnShared->last_max_csn)
		csnShared->last_max_csn = new_hlc;

	/* Handle WAL logging as in original implementation */
	if (enable_csn_wal && new_hlc > csnShared->last_csn_log_wal)
	{
		/*
		 * We log the CSN 5s greater than generated, you can see comments on
		 * the CSN_ASSIGN_TIME_INTERVAL.
		 */
		log_csn = CSNAddByNanosec(new_hlc, CSN_ASSIGN_TIME_INTERVAL);
		csnShared->last_csn_log_wal = log_csn;
	}

	if (!locked)
		SpinLockRelease(&csnShared->lock);

	if (log_csn != InvalidCSN)
		WriteAssignCSNXlogRec(new_hlc);

	return new_hlc;
}

/*
 * AdvanceHLCOnRemote - Advance HLC when receiving remote timestamp
 * 
 * This function is called when we receive a remote HLC timestamp
 * and need to advance our local HLC to maintain causality
 */
CSN
AdvanceHLCOnRemote(CSN remote_hlc)
{
	return GenerateHLCTimestamp(false, remote_hlc);
}
