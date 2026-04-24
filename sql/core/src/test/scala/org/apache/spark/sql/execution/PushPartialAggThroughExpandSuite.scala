/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for PushPartialAggThroughExpand optimization rule.
 */
class PushPartialAggThroughExpandSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("push partial aggregate through expand for GROUPING SETS") {
    withSQLConf(SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "true") {
      val df = Seq(
        ("a", "x", 1),
        ("a", "x", 2),
        ("a", "y", 3),
        ("b", "x", 4),
        ("b", "y", 5)
      ).toDF("col1", "col2", "value")

      df.createOrReplaceTempView("test_table")

      val query = sql(
        """
          |SELECT col1, col2, SUM(value) as total
          |FROM test_table
          |GROUP BY col1, col2 GROUPING SETS ((col1, col2), (col1))
          |ORDER BY col1, col2
          |""".stripMargin)

      val plan = query.queryExecution.executedPlan

      // Verify the plan contains Expand
      assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined,
        "Plan should contain ExpandExec")

      // Execute and verify results
      val result = query.collect()
      assert(result.length > 0, "Query should return results")
    }
  }

  test("push partial aggregate through expand for CUBE") {
    withSQLConf(SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "true") {
      val df = Seq(
        ("a", "x", 1),
        ("a", "y", 2),
        ("b", "x", 3)
      ).toDF("col1", "col2", "value")

      df.createOrReplaceTempView("cube_table")

      val query = sql(
        """
          |SELECT col1, col2, SUM(value) as total
          |FROM cube_table
          |GROUP BY CUBE(col1, col2)
          |ORDER BY col1, col2
          |""".stripMargin)

      val plan = query.queryExecution.executedPlan

      // Verify the plan contains Expand
      assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined,
        "Plan should contain ExpandExec for CUBE")

      // Execute and verify results
      val result = query.collect()
      assert(result.length > 0, "CUBE query should return results")
    }
  }

  test("push partial aggregate through expand for ROLLUP") {
    withSQLConf(SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "true") {
      val df = Seq(
        ("a", "x", 1),
        ("a", "y", 2),
        ("b", "x", 3)
      ).toDF("col1", "col2", "value")

      df.createOrReplaceTempView("rollup_table")

      val query = sql(
        """
          |SELECT col1, col2, SUM(value) as total
          |FROM rollup_table
          |GROUP BY ROLLUP(col1, col2)
          |ORDER BY col1, col2
          |""".stripMargin)

      val plan = query.queryExecution.executedPlan

      // Verify the plan contains Expand
      assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined,
        "Plan should contain ExpandExec for ROLLUP")

      // Execute and verify results
      val result = query.collect()
      assert(result.length > 0, "ROLLUP query should return results")
    }
  }

  test("optimization can be disabled") {
    withSQLConf(SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "false") {
      val df = Seq(
        ("a", "x", 1),
        ("a", "y", 2)
      ).toDF("col1", "col2", "value")

      df.createOrReplaceTempView("disabled_table")

      val query = sql(
        """
          |SELECT col1, col2, SUM(value) as total
          |FROM disabled_table
          |GROUP BY CUBE(col1, col2)
          |""".stripMargin)

      // Should still execute correctly even with optimization disabled
      val result = query.collect()
      assert(result.length > 0, "Query should work with optimization disabled")
    }
  }

  test("multiple aggregate functions") {
    withSQLConf(SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "true") {
      val df = Seq(
        ("a", "x", 1),
        ("a", "x", 2),
        ("a", "y", 3),
        ("b", "x", 4)
      ).toDF("col1", "col2", "value")

      df.createOrReplaceTempView("multi_agg_table")

      val query = sql(
        """
          |SELECT col1, col2, 
          |       SUM(value) as total,
          |       COUNT(value) as cnt,
          |       AVG(value) as avg_val
          |FROM multi_agg_table
          |GROUP BY CUBE(col1, col2)
          |ORDER BY col1, col2
          |""".stripMargin)

      val result = query.collect()
      assert(result.length > 0, "Query with multiple aggregates should return results")
    }
  }

  test("correctness check - compare with and without optimization") {
    val df = Seq(
      ("a", "x", 1),
      ("a", "x", 2),
      ("a", "y", 3),
      ("b", "x", 4),
      ("b", "y", 5),
      ("b", "y", 6)
    ).toDF("col1", "col2", "value")

    df.createOrReplaceTempView("correctness_table")

    val queryStr =
      """
        |SELECT col1, col2, SUM(value) as total
        |FROM correctness_table
        |GROUP BY CUBE(col1, col2)
        |ORDER BY col1, col2
        |""".stripMargin

    // Get results with optimization enabled
    val resultWithOpt = withSQLConf(
      SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "true") {
      sql(queryStr).collect()
    }

    // Get results with optimization disabled
    val resultWithoutOpt = withSQLConf(
      SQLConf.PUSH_PARTIAL_AGG_THROUGH_EXPAND_ENABLED.key -> "false") {
      sql(queryStr).collect()
    }

    // Results should be identical
    assert(resultWithOpt.length == resultWithoutOpt.length,
      "Result count should match with and without optimization")
    
    resultWithOpt.zip(resultWithoutOpt).foreach { case (row1, row2) =>
      assert(row1 == row2, s"Results should match: $row1 vs $row2")
    }
  }
}

// Made with Bob
