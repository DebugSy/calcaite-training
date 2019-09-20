package com.shiy.demo.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;

/**
 * Created by P0007 on 2019/9/19.
 */
public class VolcanoPlannerTraining {

    public static void main(String[] args) {
        CalciteSchema internalSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus rootSchema = internalSchema.plus();

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                .Builder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        //列id, 类型int
        builder.add("id", new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER));
        //列name, 类型为varchar
        builder.add("name", new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR));
        builder.add("age", new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER));
        RelDataType relDataType = builder.build();

        AbstractTable table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return relDataType;
            }
        };

        //添加表 test
        rootSchema.add("tableA", table);

        SqlParser.Config sqlParserConfig = SqlParser
                .configBuilder()
                .setLex(Lex.JAVA)
                .build();

        SqlToRelConverter.Config sqlToRelConfig = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .withInSubQueryThreshold(Integer.MAX_VALUE)
                .build();

        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(sqlParserConfig)
                .typeSystem(new ShiyTypeSystemImpl())
                .sqlToRelConverterConfig(sqlToRelConfig)
                .programs()
                .build();

        ShiyRelBuilder relBuilder = ShiyRelBuilder.create(frameworkConfig);
        relBuilder.scan("tableA");


        RexBuilder rexBuilder = new RexBuilder(relBuilder.getTypeFactory());
        RexInputRef username = relBuilder.field("name");
        RexLiteral userA = rexBuilder.makeLiteral("userA");
        RexNode rexCall = relBuilder.call(SqlStdOperatorTable.EQUALS, username, userA);
        RelNode filter = relBuilder.filter(rexCall).build();
        RelOptPlanner relOptPlanner = relBuilder.getPlanner();

        Program program =
                Programs.ofRules(
                        EnumerableRules.ENUMERABLE_SORT_RULE,
                        EnumerableRules.ENUMERABLE_VALUES_RULE,
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_FILTER_RULE);
        RelTraitSet relTraits = filter.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify();
        RelNode relNode = program.run(relOptPlanner, filter, relTraits, ImmutableList.of(), ImmutableList.of());
        System.out.println(relNode);
    }

}
