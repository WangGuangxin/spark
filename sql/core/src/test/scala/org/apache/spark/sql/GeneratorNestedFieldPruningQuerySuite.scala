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

import org.apache.spark.sql.catalyst.optimizer.GeneratorNestedFieldPruning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * End-to-end tests for [[GeneratorNestedFieldPruning]]. Every query is checked against the
 * result with the rule excluded, and the file scans are checked to read only the used
 * struct fields.
 */
class GeneratorNestedFieldPruningQuerySuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val ruleName = GeneratorNestedFieldPruning.ruleName

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

  /** The struct field names that every file scan reading the array column `column` reads. */
  private def readFields(df: DataFrame, column: String): Seq[Set[String]] =
    collect(df.queryExecution.executedPlan) {
      case s: FileSourceScanExec if s.requiredSchema.fieldNames.contains(column) =>
        s.requiredSchema(column).dataType match {
          case ArrayType(st: StructType, _) => st.fieldNames.toSet
          case other => fail(s"Unexpected type of $column: $other")
        }
    }

  test("several fields of the exploded struct are read") {
    Seq("explode", "explode_outer").foreach { gen =>
      val df = checkSameAsWithoutRule(
        s"""
           |SELECT o_key, o.flag
           |FROM orders LATERAL VIEW $gen(o_data) t AS o
           |WHERE o.qty > 3
           |""".stripMargin)
      assert(readFields(df, "o_data") === Seq(Set("qty", "flag")))
    }
  }

  test("several fields with posexplode, aggregation and join") {
    val df = checkSameAsWithoutRule(
      """
        |SELECT o.flag, count(*), sum(o.price), max(pos)
        |FROM orders JOIN parts ON p_key = o_key
        |LATERAL VIEW posexplode(o_data) t AS pos, o
        |WHERE o.qty > 1
        |GROUP BY o.flag
        |""".stripMargin)
    assert(readFields(df, "o_data") === Seq(Set("qty", "flag", "price")))
  }

  test("the whole struct is used") {
    val df = checkSameAsWithoutRule(
      """
        |SELECT o_key, o
        |FROM orders LATERAL VIEW explode(o_data) t AS o
        |WHERE o.qty > 3
        |""".stripMargin)
    assert(readFields(df, "o_data") ===
      Seq(Set("qty", "flag", "price", "comment")))
  }

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
    // Only the used fields are read, also inside the EXISTS subquery.
    assert(readFields(df, "o_data") === Seq(Set("qty", "flag", "price")))
    assert(readFields(df, "p_data") === Seq(Set("size")))
  }

  test("IN subquery over a nested table") {
    checkSameAsWithoutRule(
      """
        |SELECT o_key, o.qty
        |FROM orders LATERAL VIEW explode(o_data) t AS o
        |WHERE o.qty > 2 AND o_key IN (SELECT p_key FROM parts WHERE p_key % 3 = 0)
        |""".stripMargin)
  }

  test("nested explode of a struct field") {
    checkSameAsWithoutRule(
      """
        |SELECT o_key, o.qty, c
        |FROM orders
        |LATERAL VIEW explode(o_data) t AS o
        |LATERAL VIEW explode(split(o.flag, '')) u AS c
        |WHERE o.price > 1
        |""".stripMargin)
  }

  test("correlated scalar subquery referencing the exploded struct") {
    checkSameAsWithoutRule(
      """
        |SELECT o_key, o.qty,
        |  (SELECT count(*) FROM parts WHERE p_key = o_key AND p_key % 4 = o.qty % 4) AS cnt
        |FROM orders LATERAL VIEW explode(o_data) t AS o
        |""".stripMargin)
  }
}
