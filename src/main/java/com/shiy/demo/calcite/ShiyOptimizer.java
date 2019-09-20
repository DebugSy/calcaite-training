package com.shiy.demo.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.tools.*;

/**
 * Created by P0007 on 2019/9/19.
 */
public class ShiyOptimizer {

    private RelBuilder relBuilder;

    private RelOptPlanner planner;

    public ShiyOptimizer(RelBuilder relBuilder, RelOptPlanner planner) {
        this.relBuilder = relBuilder;
        this.planner = planner;
    }

    public RelNode optimize(RelNode relNode) {
        Program optProgram = Programs.ofRules(ruleSet);
        RelTraitSet traitSet = relNode.getTraitSet().replace(EnumerableConvention.INSTANCE);
        RelNode resultRelNode = optProgram.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());
        return resultRelNode;
    }

    RuleSet ruleSet = RuleSets.ofList(

            // push a filter into a join
            FilterJoinRule.FILTER_ON_JOIN,
            // push filter into the children of a join
            FilterJoinRule.JOIN,
            // push filter through an aggregation
            FilterAggregateTransposeRule.INSTANCE,
            // push filter through set operation
            FilterSetOpTransposeRule.INSTANCE,
            // push project through set operation
            ProjectSetOpTransposeRule.INSTANCE,

            // aggregation and projection rules
            AggregateProjectMergeRule.INSTANCE,
            AggregateProjectPullUpConstantsRule.INSTANCE,
            // push a projection past a filter or vice versa
            ProjectFilterTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            // push a projection to the children of a join
            // push all expressions to handle the time indicator correctly
            new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
            // merge projections
            ProjectMergeRule.INSTANCE,
            // remove identity project
            ProjectRemoveRule.INSTANCE,
            // reorder sort and projection
            SortProjectTransposeRule.INSTANCE,
            ProjectSortTransposeRule.INSTANCE,

            // join rules
            JoinPushExpressionsRule.INSTANCE,

            // remove union with only a single child
            UnionEliminatorRule.INSTANCE,
            // convert non-all union into all-union + distinct
            UnionToDistinctRule.INSTANCE,

            // remove aggregation if it does not aggregate and input is already distinct
            AggregateRemoveRule.INSTANCE,
            // push aggregate through join
            AggregateJoinTransposeRule.EXTENDED,
            // aggregate union rule
            AggregateUnionAggregateRule.INSTANCE,

            // reduce aggregate functions like AVG, STDDEV_POP etc.
            AggregateReduceFunctionsRule.INSTANCE,

            // remove unnecessary sort rule
            SortRemoveRule.INSTANCE,

            // prune empty results rules
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.JOIN_LEFT_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE,
            PruneEmptyRules.UNION_INSTANCE,

            // calc rules
            FilterCalcMergeRule.INSTANCE,
            ProjectCalcMergeRule.INSTANCE,
            FilterToCalcRule.INSTANCE,
            ProjectToCalcRule.INSTANCE,
            CalcMergeRule.INSTANCE
  );

}
