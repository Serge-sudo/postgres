/*-------------------------------------------------------------------------
 *
 * planmain.c
 *	  Routines to plan a single query
 *
 * What's in a name, anyway?  The top-level entry point of the planner/
 * optimizer is over in planner.c, not here as you might think from the
 * file name.  But this is the main code for planning a basic join operation,
 * shorn of features like subselects, inheritance, aggregates, grouping,
 * and so on.  (Those are the things planner.c deals with.)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planmain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/orclauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"

/* Static function declarations */
static void try_outer_join_limit_pushdown(PlannerInfo *root);
static void analyze_jointree_for_limit_pushdown(PlannerInfo *root, Node *jtnode);
static void attempt_limit_pushdown_for_outer_join(PlannerInfo *root, JoinExpr *join);
static bool can_pushdown_sort_expression(PlannerInfo *root, Node *expr, Node *preserved_side);
static void create_limited_subquery_for_preserved_side(PlannerInfo *root, JoinExpr *join, Node *preserved_side);


/*
 * query_planner
 *	  Generate a path (that is, a simplified plan) for a basic query,
 *	  which may involve joins but not any fancier features.
 *
 * Since query_planner does not handle the toplevel processing (grouping,
 * sorting, etc) it cannot select the best path by itself.  Instead, it
 * returns the RelOptInfo for the top level of joining, and the caller
 * (grouping_planner) can choose among the surviving paths for the rel.
 *
 * root describes the query to plan
 * qp_callback is a function to compute query_pathkeys once it's safe to do so
 * qp_extra is optional extra data to pass to qp_callback
 *
 * Note: the PlannerInfo node also includes a query_pathkeys field, which
 * tells query_planner the sort order that is desired in the final output
 * plan.  This value is *not* available at call time, but is computed by
 * qp_callback once we have completed merging the query's equivalence classes.
 * (We cannot construct canonical pathkeys until that's done.)
 */
RelOptInfo *
query_planner(PlannerInfo *root,
			  query_pathkeys_callback qp_callback, void *qp_extra)
{
	Query	   *parse = root->parse;
	List	   *joinlist;
	RelOptInfo *final_rel;

	/*
	 * Init planner lists to empty.
	 *
	 * NOTE: append_rel_list was set up by subquery_planner, so do not touch
	 * here.
	 */
	root->join_rel_list = NIL;
	root->join_rel_hash = NULL;
	root->join_rel_level = NULL;
	root->join_cur_level = 0;
	root->canon_pathkeys = NIL;
	root->left_join_clauses = NIL;
	root->right_join_clauses = NIL;
	root->full_join_clauses = NIL;
	root->join_info_list = NIL;
	root->placeholder_list = NIL;
	root->placeholder_array = NULL;
	root->placeholder_array_size = 0;
	root->fkey_list = NIL;
	root->initial_rels = NIL;

	/*
	 * Set up arrays for accessing base relations and AppendRelInfos.
	 */
	setup_simple_rel_arrays(root);

	/*
	 * In the trivial case where the jointree is a single RTE_RESULT relation,
	 * bypass all the rest of this function and just make a RelOptInfo and its
	 * one access path.  This is worth optimizing because it applies for
	 * common cases like "SELECT expression" and "INSERT ... VALUES()".
	 */
	Assert(parse->jointree->fromlist != NIL);
	if (list_length(parse->jointree->fromlist) == 1)
	{
		Node	   *jtnode = (Node *) linitial(parse->jointree->fromlist);

		if (IsA(jtnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jtnode)->rtindex;
			RangeTblEntry *rte = root->simple_rte_array[varno];

			Assert(rte != NULL);
			if (rte->rtekind == RTE_RESULT)
			{
				/* Make the RelOptInfo for it directly */
				final_rel = build_simple_rel(root, varno, NULL);

				/*
				 * If query allows parallelism in general, check whether the
				 * quals are parallel-restricted.  (We need not check
				 * final_rel->reltarget because it's empty at this point.
				 * Anything parallel-restricted in the query tlist will be
				 * dealt with later.)  We should always do this in a subquery,
				 * since it might be useful to use the subquery in parallel
				 * paths in the parent level.  At top level this is normally
				 * not worth the cycles, because a Result-only plan would
				 * never be interesting to parallelize.  However, if
				 * debug_parallel_query is on, then we want to execute the
				 * Result in a parallel worker if possible, so we must check.
				 */
				if (root->glob->parallelModeOK &&
					(root->query_level > 1 ||
					 debug_parallel_query != DEBUG_PARALLEL_OFF))
					final_rel->consider_parallel =
						is_parallel_safe(root, parse->jointree->quals);

				/*
				 * The only path for it is a trivial Result path.  We cheat a
				 * bit here by using a GroupResultPath, because that way we
				 * can just jam the quals into it without preprocessing them.
				 * (But, if you hold your head at the right angle, a FROM-less
				 * SELECT is a kind of degenerate-grouping case, so it's not
				 * that much of a cheat.)
				 */
				add_path(final_rel, (Path *)
						 create_group_result_path(root, final_rel,
												  final_rel->reltarget,
												  (List *) parse->jointree->quals));

				/* Select cheapest path (pretty easy in this case...) */
				set_cheapest(final_rel);

				/*
				 * We don't need to run generate_base_implied_equalities, but
				 * we do need to pretend that EC merging is complete.
				 */
				root->ec_merging_done = true;

				/*
				 * We still are required to call qp_callback, in case it's
				 * something like "SELECT 2+2 ORDER BY 1".
				 */
				(*qp_callback) (root, qp_extra);

				return final_rel;
			}
		}
	}

	/*
	 * Construct RelOptInfo nodes for all base relations used in the query.
	 * Appendrel member relations ("other rels") will be added later.
	 *
	 * Note: the reason we find the baserels by searching the jointree, rather
	 * than scanning the rangetable, is that the rangetable may contain RTEs
	 * for rels not actively part of the query, for example views.  We don't
	 * want to make RelOptInfos for them.
	 */
	add_base_rels_to_query(root, (Node *) parse->jointree);

	/*
	 * Examine the targetlist and join tree, adding entries to baserel
	 * targetlists for all referenced Vars, and generating PlaceHolderInfo
	 * entries for all referenced PlaceHolderVars.  Restrict and join clauses
	 * are added to appropriate lists belonging to the mentioned relations. We
	 * also build EquivalenceClasses for provably equivalent expressions. The
	 * SpecialJoinInfo list is also built to hold information about join order
	 * restrictions.  Finally, we form a target joinlist for make_one_rel() to
	 * work from.
	 */
	build_base_rel_tlists(root, root->processed_tlist);

	find_placeholders_in_jointree(root);

	find_lateral_references(root);

	joinlist = deconstruct_jointree(root);

	/*
	 * Reconsider any postponed outer-join quals now that we have built up
	 * equivalence classes.  (This could result in further additions or
	 * mergings of classes.)
	 */
	reconsider_outer_join_clauses(root);

	/*
	 * If we formed any equivalence classes, generate additional restriction
	 * clauses as appropriate.  (Implied join clauses are formed on-the-fly
	 * later.)
	 */
	generate_base_implied_equalities(root);

	/*
	 * We have completed merging equivalence sets, so it's now possible to
	 * generate pathkeys in canonical form; so compute query_pathkeys and
	 * other pathkeys fields in PlannerInfo.
	 */
	(*qp_callback) (root, qp_extra);

	/*
	 * Examine any "placeholder" expressions generated during subquery pullup.
	 * Make sure that the Vars they need are marked as needed at the relevant
	 * join level.  This must be done before join removal because it might
	 * cause Vars or placeholders to be needed above a join when they weren't
	 * so marked before.
	 */
	fix_placeholder_input_needed_levels(root);

	/*
	 * Remove any useless outer joins.  Ideally this would be done during
	 * jointree preprocessing, but the necessary information isn't available
	 * until we've built baserel data structures and classified qual clauses.
	 */
	joinlist = remove_useless_joins(root, joinlist);

	/*
	 * Also, reduce any semijoins with unique inner rels to plain inner joins.
	 * Likewise, this can't be done until now for lack of needed info.
	 */
	reduce_unique_semijoins(root);

	/*
	 * Now distribute "placeholders" to base rels as needed.  This has to be
	 * done after join removal because removal could change whether a
	 * placeholder is evaluable at a base rel.
	 */
	add_placeholders_to_base_rels(root);

	/*
	 * Construct the lateral reference sets now that we have finalized
	 * PlaceHolderVar eval levels.
	 */
	create_lateral_join_info(root);

	/*
	 * Match foreign keys to equivalence classes and join quals.  This must be
	 * done after finalizing equivalence classes, and it's useful to wait till
	 * after join removal so that we can skip processing foreign keys
	 * involving removed relations.
	 */
	match_foreign_keys_to_quals(root);

	/*
	 * Look for join OR clauses that we can extract single-relation
	 * restriction OR clauses from.
	 */
	extract_restriction_or_clauses(root);

	/*
	 * Now expand appendrels by adding "otherrels" for their children.  We
	 * delay this to the end so that we have as much information as possible
	 * available for each baserel, including all restriction clauses.  That
	 * let us prune away partitions that don't satisfy a restriction clause.
	 * Also note that some information such as lateral_relids is propagated
	 * from baserels to otherrels here, so we must have computed it already.
	 */
	add_other_rels_to_query(root);

	/*
	 * Distribute any UPDATE/DELETE/MERGE row identity variables to the target
	 * relations.  This can't be done till we've finished expansion of
	 * appendrels.
	 */
	distribute_row_identity_vars(root);

	/*
	 * Check if we can push down ORDER BY LIMIT to outer join relations
	 */
	try_outer_join_limit_pushdown(root);

	/*
	 * Ready to do the primary planning.
	 */
	final_rel = make_one_rel(root, joinlist);

	/* Check that we got at least one usable path */
	if (!final_rel || !final_rel->cheapest_total_path ||
		final_rel->cheapest_total_path->param_info != NULL)
		elog(ERROR, "failed to construct the join relation");

	return final_rel;
}

/*
 * try_outer_join_limit_pushdown
 *	  Attempt to push down ORDER BY LIMIT to outer join relations
 *
 * This function analyzes the query to see if we can push LIMIT and ORDER BY
 * clauses down to the preserved side of outer joins. This optimization can
 * significantly improve performance by reducing the number of rows that need
 * to be processed in the join.
 */
static void
try_outer_join_limit_pushdown(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	
	/* Only apply if we have both ORDER BY and LIMIT */
	if (!parse->sortClause || !parse->limitCount)
		return;
		
	/* Don't apply for set operations */
	if (parse->setOperations)
		return;
		
	/* Don't apply if we have OFFSET without LIMIT optimization */
	if (parse->limitOffset && !parse->limitCount)
		return;
		
	/* Don't apply if we have grouping or aggregation */
	if (parse->groupClause || parse->hasAggs || parse->havingQual)
		return;
		
	/* Don't apply if we have window functions */
	if (parse->windowClause || parse->hasWindowFuncs)
		return;
		
	/* Don't apply if we have DISTINCT */
	if (parse->distinctClause)
		return;

	/*
	 * Analyze the join tree to see if we can push down the limit.
	 * We'll walk the jointree recursively to find outer joins where
	 * the ORDER BY references only the preserved side.
	 */
	analyze_jointree_for_limit_pushdown(root, (Node *) parse->jointree);
}

/*
 * analyze_jointree_for_limit_pushdown
 *	  Recursively analyze the jointree to find opportunities for LIMIT pushdown
 */
static void
analyze_jointree_for_limit_pushdown(PlannerInfo *root, Node *jtnode)
{
	if (jtnode == NULL)
		return;
		
	if (IsA(jtnode, RangeTblRef))
	{
		/* Base relation - nothing to do */
		return;
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;
		
		/* Recursively process all items in the FROM list */
		foreach(l, f->fromlist)
		{
			analyze_jointree_for_limit_pushdown(root, lfirst(l));
		}
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;
		
		/* Check if this is an outer join where we can push down LIMIT */
		if (j->jointype == JOIN_LEFT || j->jointype == JOIN_RIGHT)
		{
			attempt_limit_pushdown_for_outer_join(root, j);
		}
		
		/* Recursively process left and right subtrees */
		analyze_jointree_for_limit_pushdown(root, j->larg);
		analyze_jointree_for_limit_pushdown(root, j->rarg);
	}
}

/*
 * attempt_limit_pushdown_for_outer_join
 *	  Try to push LIMIT down to the preserved side of an outer join
 */
static void
attempt_limit_pushdown_for_outer_join(PlannerInfo *root, JoinExpr *join)
{
	Query	   *parse = root->parse;
	Node	   *preserved_side;
	bool		can_pushdown = true;
	ListCell   *lc;
	
	/* Determine which side is preserved */
	if (join->jointype == JOIN_LEFT)
		preserved_side = join->larg;
	else /* JOIN_RIGHT */
		preserved_side = join->rarg;
		
	/*
	 * Check if ORDER BY clauses reference only columns from the preserved side.
	 * If any ORDER BY expression references the non-preserved side, we cannot
	 * push down the limit.
	 */
	foreach(lc, parse->sortClause)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, parse->targetList);
		
		if (!can_pushdown_sort_expression(root, (Node *) tle->expr, preserved_side))
		{
			can_pushdown = false;
			break;
		}
	}
	
	if (can_pushdown)
	{
		/* 
		 * We can push down the LIMIT. Create a modified subquery for the
		 * preserved side that includes the ORDER BY and LIMIT.
		 */
		create_limited_subquery_for_preserved_side(root, join, preserved_side);
	}
}

/*
 * can_pushdown_sort_expression
 *	  Check if a sort expression can be pushed down to the preserved side
 */
static bool
can_pushdown_sort_expression(PlannerInfo *root, Node *expr, Node *preserved_side)
{
	/*
	 * For now, implement a simple check: we can pushdown if the expression
	 * only references Vars that come from the preserved side.
	 * 
	 * TODO: This is a simplified implementation. A full implementation would
	 * need to analyze the preserved_side node to determine which relations
	 * it contains and check if the expression only references those relations.
	 */
	return true; /* Placeholder - always allow for now */
}

/*
 * create_limited_subquery_for_preserved_side
 *	  Create a subquery with LIMIT applied to the preserved side
 */
static void
create_limited_subquery_for_preserved_side(PlannerInfo *root, JoinExpr *join, Node *preserved_side)
{
	/*
	 * TODO: Implement the actual subquery creation logic.
	 * This would involve:
	 * 1. Creating a new Query node for the preserved side
	 * 2. Adding the appropriate ORDER BY and LIMIT clauses
	 * 3. Modifying the join tree to use the new subquery
	 * 
	 * For now, this is a placeholder that logs the attempt.
	 */
	elog(DEBUG1, "Would push down LIMIT to %s side of %s join",
		 join->jointype == JOIN_LEFT ? "left" : "right",
		 join->jointype == JOIN_LEFT ? "LEFT" : "RIGHT");
}
