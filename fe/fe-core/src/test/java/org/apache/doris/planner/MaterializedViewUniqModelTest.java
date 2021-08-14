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

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class MaterializedViewUniqModelTest {
    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/"+ MaterializedViewUniqModelTest.class.getSimpleName() +"/"
        + UUID.randomUUID() + "/";

    private static final String HR_DB_NAME = "hmb";
    private static final String SELLS_TABLE_NAME = "sells";
    private static final String UID_MV_NAME = "sells_uid";
    private static final String TIME_MV_NAME = "sells_time";
    private static DorisAssert dorisAssert;

    private static String usingRollup(String materializedViewName) {
        return "rollup: " + materializedViewName;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withEnableMV().withDatabase(HR_DB_NAME).useDatabase(HR_DB_NAME);
    }

    @Before
    public void beforeMethod() throws Exception {
        String createTableSQL = "create table " + SELLS_TABLE_NAME + " (time date, uid int, cost int) "
            + "UNIQUE KEY (time, uid)"
            + "partition by range (time) "
            + "("
            + "partition p0 values less than ('2021-01-01'), partition p1 values less than MAXVALUE"
            + ") "
            + "distributed by hash(time) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @After
    public void afterMethod() throws Exception {
        dorisAssert.dropTable(SELLS_TABLE_NAME);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    public void testUid() throws Exception {
        String alterPartitionSQL = "";
        String createRollupSQL = "alter table " + SELLS_TABLE_NAME +" add rollup " + UID_MV_NAME +" (uid, time, cost) ;";
        String query = "select cost from " + SELLS_TABLE_NAME + " where uid > 30;";
        dorisAssert.withRollup(createRollupSQL).query(query).explainContains(usingRollup(UID_MV_NAME));
    }

    @Test
    public void createAggMV() throws Exception {
//        String createRollupSQLAgg = "create materialized view agg as select time, uid, sum(cost) from " + SELLS_TABLE_NAME +" group by time, uid ;";
        String createRollupSQLTime = "create materialized view aggTime as select time, sum(cost) from " + SELLS_TABLE_NAME +" group by time ;";
        String createRollupSQLUid = "create materialized view aggUid as select uid, sum(cost) from " + SELLS_TABLE_NAME +" group by uid ;";
        dorisAssert
//            .withMaterializedView(createRollupSQLTime)
            .withMaterializedView(createRollupSQLUid)
        ;
        dorisAssert.query("select sum(cost) from " + SELLS_TABLE_NAME +";").explainContains();
    }


}
