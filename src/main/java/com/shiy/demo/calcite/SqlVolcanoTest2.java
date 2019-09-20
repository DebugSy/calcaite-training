package com.shiy.demo.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by P0007 on 2019/9/19.
 */
public class SqlVolcanoTest2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlVolcanoTest2.class);

    public static void main(String[] args) {
        SchemaPlus rootSchema = CalciteUtils.registerRootSchema();

        SqlParser.Config sqlParserConfig = SqlParser
                .configBuilder()
                .setLex(Lex.JAVA)
                .build();

        final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig)
                .defaultSchema(rootSchema)
                .build();

        ShiyRelBuilder2 relBuilder = ShiyRelBuilder2.create(fromworkConfig);
        relBuilder.scan("USERS");
        RexBuilder rexBuilder = new RexBuilder(relBuilder.getTypeFactory());
        RexInputRef username = relBuilder.field("NAME");
        RexLiteral userA = rexBuilder.makeLiteral("userA");
        RexNode rexCall = relBuilder.call(SqlStdOperatorTable.EQUALS, username, userA);
        RelNode filter = relBuilder.filter(rexCall).build();

        RelOptPlanner planner = relBuilder.getPlanner();
//        planner.clear();
//        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // add ConverterRule
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);


        RelTraitSet desiredTraits =
                filter.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        RelNode changeTraits = planner.changeTraits(filter, desiredTraits);

        planner.setRoot(changeTraits);
        RelNode bestExp = planner.findBestExp();
        System.out.println("-----------------------------------------------------------");
        System.out.println("The Best relational expression string:");
        System.out.println(RelOptUtil.toString(bestExp));
        System.out.println("-----------------------------------------------------------");

    }
}
