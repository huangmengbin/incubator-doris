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

public class TestAggregateRollup {
    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/"+ TestAggregateRollup.class.getSimpleName() +"/"
        + UUID.randomUUID() + "/";
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withEnableMV().withDatabase("hmb").useDatabase("hmb");
    }

    @Before
    public void beforeMethod() throws Exception {
        String createTableSQL = "CREATE TABLE aggregate_rollup (\n" +
            "\t\t`user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "\t\t`date` DATE NOT NULL COMMENT \"数据灌入日期时间\",\n" +
            "\t\t`city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "\t\t`age` SMALLINT COMMENT \"用户年龄\",\n" +
            "\t\t`sex` TINYINT COMMENT \"用户性别\",\n" +
            "\t\t`last_visit_date` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户最后一次访问时间\",\n" +
            "\t\t`cost` BIGINT SUM DEFAULT \"0\" COMMENT \"用户总消费\",\n" +
            "\t\t`max_dwell_time` INT MAX DEFAULT \"0\" COMMENT \"用户最大停留时间\",\n" +
            "\t\t`min_dwell_time` INT MIN DEFAULT \"99999\" COMMENT \"用户最小停留时间\"\n" +
            "\t) AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`) BUCKETS 24 PROPERTIES (\n" +
            "\t\t\"replication_num\" = \"1\",\n" +
            "\t\t\"in_memory\" = \"false\",\n" +
            "\t\t\"business_key_column_name\" = \"user_id\",\n" +
            "\t\t\"storage_medium\" = \"HDD\",\n" +
            "\t\t\"storage_format\" = \"V2\"\n" +
            "\t);";
        dorisAssert.withTable(createTableSQL);
    }

    @After
    public void afterMethod() throws Exception {
        dorisAssert.dropTable("aggregate_rollup");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    public void test() throws Exception {
        String createRollupSQL = "alter table aggregate_rollup add rollup uid_rollup(user_id, cost); ";
        dorisAssert.withRollup(createRollupSQL);
        dorisAssert.query("select     cost  from aggregate_rollup where user_id = 1 ;").explainContains("rollup: aggregate_rollup");
        dorisAssert.query("select sum(cost) from aggregate_rollup where user_id = 1 ;").explainContains("rollup: uid_rollup");
        dorisAssert.query("select max(cost) from aggregate_rollup where user_id = 1 ;").explainContains("rollup: aggregate_rollup");

    }

}
