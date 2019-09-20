package com.shiy.demo.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableBindable;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by P0007 on 2019/9/19.
 */
public class ShiyRelBuilder extends RelBuilder {

    protected ShiyRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public RelOptPlanner getPlanner() {
        RelOptPlanner relOptPlanner = cluster.getPlanner();
        return relOptPlanner;
    }

    private static final List<RelOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    AggregateStarTableRule.INSTANCE,
                    AggregateStarTableRule.INSTANCE2,
                    TableScanRule.INSTANCE,
                    JoinAssociateRule.INSTANCE,
                    ProjectMergeRule.INSTANCE,
                    FilterTableScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    AggregateReduceFunctionsRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    ProjectWindowTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    SortProjectTransposeRule.INSTANCE,
                    SortJoinTransposeRule.INSTANCE,
                    SortRemoveConstantKeysRule.INSTANCE,
                    SortUnionTransposeRule.INSTANCE);


    /** Creates a RelBuilder. */
    public static ShiyRelBuilder create(FrameworkConfig config) {
        RelDataTypeSystem typeSystem = config.getTypeSystem();
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(typeSystem);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(config.getExecutor());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        for (RelOptRule rule : DEFAULT_RULES) {
//            planner.addRule(rule);
        }
        planner.addRule(TableScanRule.INSTANCE);


        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        CalciteSchema calciteSchema = CalciteSchema.from(config.getDefaultSchema());

        Properties properties = new Properties();
        SqlParser.Config parserConfig = config.getParserConfig();
        properties.setProperty(
                CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(parserConfig.caseSensitive()));
        CalciteConnectionConfigImpl calciteConnectionConfig = new CalciteConnectionConfigImpl(properties);

        CalciteCatalogReader relOptSchema = new CalciteCatalogReader(
                calciteSchema,
                Collections.emptyList(),
                typeFactory,
                calciteConnectionConfig);
        return new ShiyRelBuilder(config.getContext(), cluster, relOptSchema);
    }



}
