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

import org.apache.spark.sql.catalyst.expressions.ArrayExists
import org.apache.spark.sql.catalyst.optimizer.GeneratorArrayPrefilter
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for [[GeneratorArrayPrefilter]]. Every query is checked against the
 * result with the rule excluded. The tables contain null arrays, empty arrays and null
 * elements.
 */
class GeneratorArrayPrefilterQuerySuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val ruleName = GeneratorArrayPrefilter.ruleName

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

  test("filter on the exploded struct is applied to the array") {
    val df = checkSameAsWithoutRule(
      """
        |SELECT o_key, o.price
        |FROM orders LATERAL VIEW explode(o_data) t AS o
        |WHERE o.qty = 4 AND o.flag = 'B'
        |""".stripMargin)
    // The array-level copy of the filter is pushed into the scan side, below the generate.
    val generate = optimized(df).collectFirst { case g: Generate => g }.get
    assert(generate.child.exists(_.expressions.exists(_.exists {
      case _: ArrayExists => true
      case _ => false
    })), s"No array prefilter below the generate:\n${optimized(df)}")
  }

  test("filters with posexplode and with conjuncts that are not copied") {
    checkSameAsWithoutRule(
      """
        |SELECT o_key, pos, o.price
        |FROM orders LATERAL VIEW posexplode(o_data) t AS pos, o
        |WHERE o.qty > 2 AND pos > 0 AND o_key % 2 = 0
        |""".stripMargin)
  }

  test("filters on explode_outer are not copied") {
    val df = checkSameAsWithoutRule(
      """
        |SELECT o_key, o.price
        |FROM orders LATERAL VIEW explode_outer(o_data) t AS o
        |WHERE o.qty = 4 OR o IS NULL
        |""".stripMargin)
    val generate = optimized(df).collectFirst { case g: Generate => g }.get
    assert(!generate.child.exists(_.expressions.exists(_.exists {
      case _: ArrayExists => true
      case _ => false
    })), s"Array prefilter below an outer generate:\n${optimized(df)}")
  }

  test("filters on fields that can be null") {
    checkSameAsWithoutRule(
      """
        |SELECT o_key, o.qty
        |FROM orders LATERAL VIEW explode(o_data) t AS o
        |WHERE NOT (o.flag = 'A') AND o.price IS NOT NULL
        |""".stripMargin)
  }
}
