package com.shiy.demo.calcite;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by P0007 on 2019/9/19.
 */
public class ShiyRelBuilder2 extends RelBuilder {

    protected ShiyRelBuilder2(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public RelOptPlanner getPlanner() {
        RelOptPlanner relOptPlanner = cluster.getPlanner();
        return relOptPlanner;
    }


    /** Creates a RelBuilder. */
    public static ShiyRelBuilder2 create(FrameworkConfig config) {
        final RelOptCluster[] clusters = {null};
        final RelOptSchema[] relOptSchemas = {null};
        Frameworks.withPrepare(
                new Frameworks.PrepareAction<Void>(config) {
                    public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
                                      SchemaPlus rootSchema, CalciteServerStatement statement) {
                        clusters[0] = cluster;
                        relOptSchemas[0] = relOptSchema;
                        return null;
                    }
                });
        return new ShiyRelBuilder2(config.getContext(), clusters[0], relOptSchemas[0]);
    }



}
