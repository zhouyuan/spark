package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.test.SharedSparkSession

class OptimizeRollupPrefixAggSuite extends QueryTest with SharedSparkSession {

  test("rollupPrefixAgg removes ExpandExec for wide rollup") {
    withSQLConf(
      "spark.sql.optimizer.rollupPrefixAgg.enabled" -> "true",
      "spark.sql.optimizer.rollupPrefixAgg.minKeys" -> "5") {

      val df = spark.range(2000)
        .selectExpr(
          "id % 3 as a",
          "id % 5 as b",
          "id % 7 as c",
          "id % 11 as d",
          "id % 13 as e",
          "id % 17 as f",
          "cast(id as long) as v")
      df.createOrReplaceTempView("t_rollup_opt")

      val q = spark.sql("""
        select a,b,c,d,e,f, sum(v) as s
        from t_rollup_opt
        group by rollup(a,b,c,d,e,f)
      """)

      val expands = q.queryExecution.executedPlan.collect { case e: ExpandExec => e }
      assert(expands.isEmpty)
    }
  }

  test("rollupPrefixAgg preserves results") {
    val df = spark.range(2000)
      .selectExpr(
        "id % 3 as a",
        "id % 5 as b",
        "id % 7 as c",
        "id % 11 as d",
        "id % 13 as e",
        "id % 17 as f",
        "cast(id as long) as v")
    df.createOrReplaceTempView("t_rollup_opt2")

    val sqlText = """
      select a,b,c,d,e,f, sum(v) as s
      from t_rollup_opt2
      group by rollup(a,b,c,d,e,f)
      order by a,b,c,d,e,f
    """

    val baseline = withSQLConf(
      "spark.sql.optimizer.rollupPrefixAgg.enabled" -> "false") {
      spark.sql(sqlText).collect().toSeq
    }

    val optimized = withSQLConf(
      "spark.sql.optimizer.rollupPrefixAgg.enabled" -> "true",
      "spark.sql.optimizer.rollupPrefixAgg.minKeys" -> "5") {
      spark.sql(sqlText).collect().toSeq
    }

    assert(baseline == optimized)
  }
}