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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.optimizer.PushGenerateBelowSemiAntiJoin
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Join, LogicalPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for [[PushGenerateBelowSemiAntiJoin]]. Every query is checked against
 * the result with the rule excluded, and the optimized plan is checked for where the
 * generate ends up.
 */
class PushGenerateBelowSemiAntiJoinQuerySuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val ruleName = PushGenerateBelowSemiAntiJoin.ruleName

  override def beforeAll(): Unit = {
    super.beforeAll()
    // A small nested layout in the style of nested TPC-H: keys as top-level columns, everything
    // else in an array of structs. Includes null arrays, empty arrays and null elements.
    sql(
      """
        |CREATE TABLE orders USING parquet AS
        |SELECT id AS o_key,
        |  CASE
        |    WHEN id % 13 = 0 THEN NULL
        |    WHEN id % 11 = 0 THEN array()
        |    ELSE transform(sequence(0, CAST(id % 5 AS INT)), i ->
        |      IF(id % 7 = 0 AND i = 1, NULL,
        |        named_struct('qty', CAST((id + i) % 10 AS INT), 'flag', IF(i % 2 = 0, 'A', 'B'),
        |          'price', (id * i) / 3.0D, 'comment', repeat('x', 20))))
        |  END AS o_data
        |FROM range(0, 200)
        |""".stripMargin)
    sql(
      """
        |CREATE TABLE parts USING parquet AS
        |SELECT id % 150 AS p_key,
        |  array(named_struct('size', CAST(id % 4 AS INT), 'name', CAST(id AS STRING),
        |    'comment', repeat('y', 20))) AS p_data
        |FROM range(0, 300)
        |""".stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS orders")
      sql("DROP TABLE IF EXISTS parts")
    } finally {
      super.afterAll()
    }
  }

  private def checkSameAsWithoutRule(query: String): DataFrame = {
    val expected = withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
      sql(query).collect().toSeq
    }
    val df = sql(query)
    checkAnswer(df, expected)
    df
  }

  private def optimized(df: DataFrame): LogicalPlan = df.queryExecution.optimizedPlan

  test("EXISTS and NOT EXISTS over a nested table") {
    // Shaped like TPC-H q21/q22 on the nested layout.
    val df = checkSameAsWithoutRule(
      """
        |SELECT o_key, o.qty, o.price
        |FROM orders LATERAL VIEW explode(o_data) t AS o
        |WHERE o.flag = 'A'
        |  AND EXISTS (
        |    SELECT * FROM parts LATERAL VIEW explode(p_data) s AS p
        |    WHERE p_key = o_key AND p.size > 1)
        |  AND NOT EXISTS (
        |    SELECT * FROM parts WHERE p_key = o_key + 100)
        |""".stripMargin)
    val plan = optimized(df)
    val joins = plan.collect { case j: Join => j }
    assert(joins.map(_.joinType).toSet === Set(LeftSemi, LeftAnti))
    // The explode runs below the semi/anti joins, so the array is not carried into them.
    joins.foreach { j =>
      assert(!j.output.exists(_.name == "o_data"), s"Array carried into a join:\n$plan")
      assert(j.left.exists(_.isInstanceOf[Generate]), s"Generate above a join:\n$plan")
    }
  }

  test("IN and NOT IN subqueries") {
    Seq("IN", "NOT IN").foreach { in =>
      checkSameAsWithoutRule(
        s"""
           |SELECT o_key, o.qty
           |FROM orders LATERAL VIEW explode(o_data) t AS o
           |WHERE o.qty > 2 AND o_key $in (SELECT p_key FROM parts WHERE p_key % 3 = 0)
           |""".stripMargin)
    }
  }

  test("plans without generators are unchanged") {
    val query =
      """
        |SELECT o_key FROM orders
        |WHERE EXISTS (SELECT * FROM parts WHERE p_key = o_key)
        |  AND NOT EXISTS (SELECT * FROM parts WHERE p_key = o_key + 1)
        |""".stripMargin
    val expected = withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
      optimized(sql(query))
    }
    comparePlans(optimized(sql(query)), expected, checkAnalysis = false)
  }
}
