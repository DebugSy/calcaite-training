package com.shiy.demo.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by P0007 on 2019/9/19.
 */
public class SqlVolcanoTest3 {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlVolcanoTest3.class);

    public static void main(String[] args) {
        SchemaPlus rootSchema = CalciteUtils.registerRootSchema();

        final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .build();

        ShiyRelBuilder relBuilder = ShiyRelBuilder.create(fromworkConfig);
        relBuilder.scan("USERS");
        RexBuilder rexBuilder = new RexBuilder(relBuilder.getTypeFactory());
        RexInputRef username = relBuilder.field("NAME");
        RexLiteral userA = rexBuilder.makeLiteral("userA");
        RexNode rexCall = relBuilder.call(SqlStdOperatorTable.EQUALS, username, userA);
        RelNode filter = relBuilder.filter(rexCall).build();


        RelTraitSet desiredTraits =
                filter.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);

        RelOptPlanner planner = relBuilder.getPlanner();

        Program program =
                Programs.ofRules(
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_FILTER_RULE);
        RelNode relNode = program.run(planner, filter, desiredTraits, ImmutableList.of(), ImmutableList.of());

        System.out.println("-----------------------------------------------------------");
        System.out.println("The Best relational expression string:");
        System.out.println(RelOptUtil.toString(relNode));
        System.out.println("-----------------------------------------------------------");

    }
}
