// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class RangePartitionPrunerTest {


    private static final String runningDir = "fe/mocked/RangePartitionPrunerTest/" + UUID.randomUUID() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        String sql = "CREATE TABLE test.`prune1` (\n" +
            "  `a` int(11) NULL COMMENT \"\",\n" +
            "  `b` bigint(20) NULL COMMENT \"\",\n" +
            "  `c` tinyint(4) NULL COMMENT \"\",\n" +
            "  `d` int(11) NULL COMMENT \"\"\n" +
            ")\n" +
            "UNIQUE KEY(`a`,`b`,`c`)\n" +
            "COMMENT \"OLAP\"\n" +
            "PARTITION BY RANGE(`a`,`b`,`c`)(\n" +
            "  PARTITION pMin VALUES LESS THAN ('0'),\n" +
            "  PARTITION p000 VALUES [('0','0','0'),('0','0','2')),\n" +
            "  PARTITION p002 VALUES [('0','0','2'),('0','0','4')),\n" +
            "  PARTITION p004 VALUES [('0','0','4'),('0','2','0')),\n" +
            "  PARTITION p020 VALUES [('0','2','0'),('0','2','2')),\n" +
            "  PARTITION p022 VALUES [('0','2','2'),('0','2','4')),\n" +
            "  PARTITION p024 VALUES [('0','2','4'),('0','4','0')),\n" +
            "  PARTITION p040 VALUES [('0','4','0'),('0','4','2')),\n" +
            "  PARTITION p042 VALUES [('0','4','2'),('0','4','4')),\n" +
            "  PARTITION p044 VALUES [('0','4','4'),('2','0','0')),\n" +
            "  PARTITION p200 VALUES [('2','0','0'),('2','0','2')),\n" +
            "  PARTITION p202 VALUES [('2','0','2'),('2','0','4')),\n" +
            "  PARTITION p4to6 VALUES [('4'),('6'))" +
            ")\n" +
            "DISTRIBUTED BY HASH(`a`) BUCKETS 2 \n" +
            "PROPERTIES(\"replication_num\" = \"1\");";
        createTable(sql);

    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @Before
    public void before() {
        FeConstants.runningUnitTest = true;
    }

    @After
    public void after() {
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGeOperator() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a >= 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=12/13"));
        Assert.assertFalse(explainString.contains("partitions=13/13"));
    }

    @Test
    public void testGtOperator() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a > 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=4/13"));
    }

    @Test
    public void testEqOperator() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));
    }

    @Test
    public void testInPredicate() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a in (0);";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));
    }


    @Test
    public void test1() throws Exception {
        String queryStr = "explain select * from test.`prune1` where b > 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=13/13"));
    }

    @Test
    public void test2() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a >= 0 and a < 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=0/13"));
    }

    @Test
    public void test3() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));
    }

    @Test
    public void test4() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a >=0 and a <= 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));
    }

    @Test
    public void test5() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a < 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=1/13"));
    }

    @Test
    public void test6() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a <= 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=10/13"));
    }

    @Test
    public void test7() throws Exception {
        String queryStr = "explain select * from test.`prune1`;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=13/13"));
    }

    @Test
    public void test8() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a in(0,1);";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));
    }

    @Test
    public void testErase1() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b = 0;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
    }

    @Test
    public void testErase2() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b <= 4;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase3() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b < 4;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase4() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b < -4;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase5() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b <= 2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase6() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b < 2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase7() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b <= 1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase8() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b < 1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase9() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 1 and b < 1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase10() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b > 0  and b < 2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase11() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b >= 0 and b < 2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase12() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b >= 0 and b <= 2;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

    @Test
    public void testErase13() throws Exception {
        String queryStr = "explain select * from test.`prune1` where a = 0 and b = 0 and c between 0 and 2 and c < 3;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    }

}
