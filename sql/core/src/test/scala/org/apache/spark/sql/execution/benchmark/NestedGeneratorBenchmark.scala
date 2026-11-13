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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.optimizer.{GeneratorArrayPrefilter,
  GeneratorNestedFieldPruning, PushGenerateBelowSemiAntiJoin}
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark for queries that `explode` an array of structs, as in a nested TPC-H layout where
 * every table keeps its keys as top-level columns and all other columns in one
 * `ARRAY<STRUCT<...>>`. Each case runs with and without the optimizer rules
 * [[GeneratorNestedFieldPruning]], [[GeneratorArrayPrefilter]] and
 * [[PushGenerateBelowSemiAntiJoin]].
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/NestedGeneratorBenchmark-results.txt".
 * }}}
 */
object NestedGeneratorBenchmark extends SqlBasedBenchmark {

  private val numRows = 2 * 1000 * 1000
  private val numIters = 3

  private val ruleNames = Seq(
    GeneratorNestedFieldPruning, GeneratorArrayPrefilter, PushGenerateBelowSemiAntiJoin
  ).map(_.ruleName).mkString(",")

  private def prepareTables(dir: String): Unit = {
    // 4 elements per row on average, with 8 fields of which the queries use 2 or 3.
    spark.sql(
      s"""
         |SELECT id AS o_key,
         |  transform(sequence(0, CAST(id % 7 AS INT)), i -> named_struct(
         |    'qty', CAST((id + i) % 50 AS INT),
         |    'price', (id * 7 + i) % 100000 / 100.0D,
         |    'flag', IF((id + i) % 3 = 0, 'A', 'B'),
         |    'shipdate', date_add(DATE'1995-01-01', CAST((id + i) % 2000 AS INT)),
         |    'commitdate', date_add(DATE'1995-01-01', CAST((id * 3 + i) % 2000 AS INT)),
         |    'mode', concat('mode', CAST(i % 7 AS STRING)),
         |    'instruct', repeat('i', 25),
         |    'comment', repeat(CAST(id AS STRING), 4))) AS o_data
         |FROM range(0, $numRows)
         |""".stripMargin).write.parquet(s"$dir/orders")
    spark.sql(
      s"""
         |SELECT id * 3 AS p_key,
         |  array(named_struct('size', CAST(id % 50 AS INT), 'comment', repeat('p', 40)))
         |    AS p_data
         |FROM range(0, ${numRows / 3})
         |""".stripMargin).write.parquet(s"$dir/parts")
    spark.read.parquet(s"$dir/orders").createOrReplaceTempView("orders")
    spark.read.parquet(s"$dir/parts").createOrReplaceTempView("parts")
  }

  private def runCase(name: String, query: String): Unit = {
    val benchmark = new Benchmark(name, numRows, numIters, output = output)
    benchmark.addCase("without nested generator rules") { _ =>
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleNames) {
        spark.sql(query).noop()
      }
    }
    benchmark.addCase("with nested generator rules") { _ =>
      spark.sql(query).noop()
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true") {
        prepareTables(dir.getCanonicalPath)

        runBenchmark("Explode an array of structs") {
          runCase("Two fields used",
            """
              |SELECT o_key, o.price
              |FROM orders LATERAL VIEW explode(o_data) t AS o
              |WHERE o.shipdate > o.commitdate
              |""".stripMargin)

          runCase("Three fields used, aggregated",
            """
              |SELECT o.flag, sum(o.price), avg(o.qty)
              |FROM orders LATERAL VIEW explode(o_data) t AS o
              |WHERE o.qty < 25
              |GROUP BY o.flag
              |""".stripMargin)
        }

        runBenchmark("Explode an array of structs below EXISTS / NOT EXISTS") {
          runCase("EXISTS and NOT EXISTS",
            """
              |SELECT o.flag, count(*), sum(o.price)
              |FROM orders LATERAL VIEW explode(o_data) t AS o
              |WHERE o.shipdate > o.commitdate
              |  AND EXISTS (
              |    SELECT * FROM parts LATERAL VIEW explode(p_data) s AS p
              |    WHERE p_key = o_key AND p.size > 10)
              |  AND NOT EXISTS (
              |    SELECT * FROM parts WHERE p_key = o_key + 1)
              |GROUP BY o.flag
              |""".stripMargin)
        }
      }
    }
  }
}
