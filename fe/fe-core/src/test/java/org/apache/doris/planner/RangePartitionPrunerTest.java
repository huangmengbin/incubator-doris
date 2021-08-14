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
import org.apache.doris.common.Config;
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

        String sql0 = "CREATE TABLE test.`prune0` (\n" +
            "   `a` int(11) NULL COMMENT \"\",\n" +
            "   `b` bigint(20) NULL COMMENT \"\"\n" +
            ")\n" +
            "UNIQUE KEY(`a`)\n" +
            "COMMENT \"OLAP\"\n" +
            "PARTITION BY RANGE(`a`)(\n" +
            "  PARTITION p0 VALUES [('0'),('2')),\n" +
            "  PARTITION p2 VALUES [('2'),('4'))\n" +
            ")\n" +
            "DISTRIBUTED BY HASH(`a`) BUCKETS 2 \n" +
            "PROPERTIES(\"replication_num\" = \"1\");";

        String sql1 = "CREATE TABLE test.`prune1` (\n" +
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

        String sql2 = "CREATE TABLE test.`TestPartitionByDate` (\n" +
            "\t`p_date` date NULL,\n" +
            "\t`cost` int(11) SUM NULL \n" +
            ")aggregate KEY(`p_date`)\n" +
            "PARTITION BY RANGE(`p_date`)(\n" +
            "\tPARTITION p20210801 VALUES [('2021-08-01'),('2021-08-02')),\n" +
            "\tPARTITION p20210802 VALUES [('2021-08-02'),('2021-08-03'))\n" +
            ")DISTRIBUTED BY HASH(`p_date`) BUCKETS 2\n" +
            "PROPERTIES(\"replication_num\" = \"1\");";
        createTable(sql0);
        createTable(sql1);
        createTable(sql2);
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
        Config.enable_erase_where_expr_after_partition_prune = true;
    }

    @After
    public void after() {
        FeConstants.runningUnitTest = false;
        // The default enable_erase_where_expr_after_partition_prune is false.
        Config.enable_erase_where_expr_after_partition_prune = false;
    }

    @Test
    public void testPartitionPrune() throws Exception {
        // all
        String queryStr = "explain select * from test.`prune1`;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=13/13"));

        // eq operator
        queryStr = "explain select * from test.`prune1` where a = 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));

        // lt operator
        queryStr = "explain select * from test.`prune1` where a < 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=1/13"));

        // le operator
        queryStr = "explain select * from test.`prune1` where a <= 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=10/13"));

        // gt operator
        queryStr = "explain select * from test.`prune1` where a > 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=4/13"));

        // ge operator
        queryStr = "explain select * from test.`prune1` where a >= 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=12/13"));
        Assert.assertFalse(explainString.contains("partitions=13/13"));

        queryStr = "explain select * from test.`prune1` where a >= 0 and a < 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=0/13"));

        queryStr = "explain select * from test.`prune1` where a >=0 and a <= 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));

        // in operator
        queryStr = "explain select * from test.`prune1` where a in (0);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));

        queryStr = "explain select * from test.`prune1` where a in(0,1);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=9/13"));

        // second column
        queryStr = "explain select * from test.`prune1` where b > 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("partitions=13/13"));
    }

    @Test
    public void testOneColumnErase() throws Exception {
        String explainString;

        String gt0 = "explain select b from test.`prune0` where a > 0";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, gt0);
        Assert.assertTrue(explainString.contains("`a` > 0"));

        String gt1 = "explain select b from test.`prune0` where a > 1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, gt1);
        Assert.assertTrue(explainString.contains("`a` > 1"));

        String gt2 = "explain select b from test.`prune0` where a > 2";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, gt2);
        Assert.assertTrue(explainString.contains("`a` > 2"));

        String gt4 = "explain select b from test.`prune0` where a > 4";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, gt4);
        Assert.assertTrue(explainString.contains("`a` > 4"));

        String ge0 = "explain select b from test.`prune0` where a >= 0";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, ge0);
        Assert.assertFalse(explainString.contains("`a` >= 0"));

        String ge1 = "explain select b from test.`prune0` where a >= 1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, ge1);
        Assert.assertTrue(explainString.contains("`a` >= 1"));

        String ge2 = "explain select b from test.`prune0` where a >= 2";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, ge2);
        Assert.assertFalse(explainString.contains("`a` >= 2"));

        String ge4 = "explain select b from test.`prune0` where a >= 4";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, ge4);
        Assert.assertTrue(explainString.contains("`a` >= 4"));

        String lt0 = "explain select b from test.`prune0` where a < 0";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, lt0);
        Assert.assertTrue(explainString.contains("`a` < 0"));

        String lt1 = "explain select b from test.`prune0` where a < 1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, lt1);
        Assert.assertTrue(explainString.contains("`a` < 1"));

        String lt2 = "explain select b from test.`prune0` where a < 2";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, lt2);
        Assert.assertFalse(explainString.contains("`a` < 2"));

        String lt4 = "explain select b from test.`prune0` where a < 4";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, lt4);
        Assert.assertFalse(explainString.contains("`a` < 4"));

        String le0 = "explain select b from test.`prune0` where a <= 0";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, le0);
        Assert.assertTrue(explainString.contains("`a` <= 0"));

        String le1 = "explain select b from test.`prune0` where a <= 1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, le1);
        Assert.assertTrue(explainString.contains("`a` <= 1"));

        String le2 = "explain select b from test.`prune0` where a <= 2";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, le2);
        Assert.assertTrue(explainString.contains("`a` <= 2"));

        String le4 = "explain select b from test.`prune0` where a <= 4";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, le4);
        Assert.assertFalse(explainString.contains("`a` <= 4"));

        String eq0 = "explain select b from test.`prune0` where a = 0";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, eq0);
        Assert.assertTrue(explainString.contains("`a` = 0"));

        String eq1 = "explain select b from test.`prune0` where a = 1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, eq1);
        Assert.assertTrue(explainString.contains("`a` = 1"));

        String eq2 = "explain select b from test.`prune0` where a = 2";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, eq2);
        Assert.assertTrue(explainString.contains("`a` = 2"));

        String eq4 = "explain select b from test.`prune0` where a = 4";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, eq4);
        Assert.assertTrue(explainString.contains("`a` = 4"));

        String all = "explain select b from test.`prune0`;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, all);
        Assert.assertFalse(explainString.contains("errCode"));

        String none = "explain select b from test.`prune0` where a>=2 and a<2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, none);
        Assert.assertTrue(explainString.contains("`a` >= 2") && explainString.contains("`a` < 2"));

        String err = "explain select b from test.`prune0` where a>2 and a<2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, err);
        Assert.assertTrue(explainString.contains("`a` > 2") && explainString.contains("`a` < 2"));

    }

    @Test
    public void testThreeColumnErase() throws Exception {
        String queryStr, explainString;

        // p000 ~ p004
        queryStr = "explain select * from test.`prune1` where a = 0 and b = 0;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` = 0"));  // (0,1,0) in p044

        // p000 ~ p044
        queryStr = "explain select * from test.`prune1` where a = 0 and b <= 4;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`a` = 0"));  // (1,0,0) in p044
        Assert.assertTrue(explainString.contains("`b` <= 4"));  // (0,5,0) in p044

        // p000 ~ p024
        queryStr = "explain select * from test.`prune1` where a = 0 and b < 4;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` < 4"));  // (0,4,-1) in p024

        // empty
        queryStr = "explain select * from test.`prune1` where a = 0 and b < -4;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` < -4"));

        // p000 ~ p024
        queryStr = "explain select * from test.`prune1` where a = 0 and b <= 2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` <= 2"));  // (0,3,0) in p024

        // p000 ~ p004
        queryStr = "explain select * from test.`prune1` where a = 0 and b < 2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` < 2"));  // (0,2,-1) in p004

        // p000 ~ p004
        queryStr = "explain select * from test.`prune1` where a = 0 and b <= 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` <= 1"));  // (0,2,-1) in p004

        // p000 ~ p004
        queryStr = "explain select * from test.`prune1` where a = 0 and b < 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` < 1"));  // (0,1,0) in p004

        // p044
        queryStr = "explain select * from test.`prune1` where a = 1 and b < 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`a` = 1"));  // (0,4,4) in p044
        Assert.assertTrue(explainString.contains("`b` < 1"));  // (0,4,4) in p044

        // p000 ~ p004
        queryStr = "explain select * from test.`prune1` where a = 0 and b > 0  and b < 2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertTrue(explainString.contains("`b` > 0"));  // (0,0,0) in p000
        Assert.assertTrue(explainString.contains("`b` < 2"));  // (0,2,-1) in p044

        // p000 ~ p004
        queryStr = "explain select * from test.`prune1` where a = 0 and b >= 0 and b < 2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertFalse(explainString.contains("`b` >= 0"));
        Assert.assertTrue(explainString.contains("`b` < 2"));  // (0,2,-1) in p044

        // p000 ~ p024
        queryStr = "explain select * from test.`prune1` where a = 0 and b >= 0 and b <= 2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertFalse(explainString.contains("`b` >= 0"));
        Assert.assertTrue(explainString.contains("`b` <= 2"));  // (0,3,0) in p024

        // p000、p002
        queryStr = "explain select * from test.`prune1` where a = 0 and b = 0 and c < 4;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`a` = 0"));
        Assert.assertFalse(explainString.contains("`b` = 0"));
        Assert.assertFalse(explainString.contains("`c` < 4"));
    }

    @Test
    public void testPartitionByDate() throws Exception {
        String queryStr, explainString;

        queryStr = "explain select * from test.TestPartitionByDate where p_date = '2021-08-01'";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("`p_date` = '2021-08-01 00:00:00'"));

        queryStr = "explain select * from test.TestPartitionByDate where p_date >= '2021-08-01' and p_date < '2021-08-02'";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertFalse(explainString.contains("`p_date` >= "));
        Assert.assertFalse(explainString.contains("`p_date` < "));
    }
}
