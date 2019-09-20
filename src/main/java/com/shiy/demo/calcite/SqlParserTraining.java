package com.shiy.demo.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Created by P0007 on 2019/9/17.
 */
public class SqlParserTraining {
    public static void main(String[] args) throws SqlParseException {
        String sql = "select id,name from dual";
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        System.out.println(sqlSelect);
    }
}
