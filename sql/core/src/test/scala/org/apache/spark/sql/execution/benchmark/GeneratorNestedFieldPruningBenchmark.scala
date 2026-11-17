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
import org.apache.spark.sql.catalyst.optimizer.GeneratorNestedFieldPruning
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark for queries that `explode` an array of structs, as in a nested TPC-H layout where
 * every table keeps its keys as top-level columns and all other columns in one
 * `ARRAY<STRUCT<...>>`. Each case runs with and without the optimizer rule
 * [[GeneratorNestedFieldPruning]].
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/GeneratorNestedFieldPruningBenchmark-results.txt".
 * }}}
 */
object GeneratorNestedFieldPruningBenchmark extends SqlBasedBenchmark {

  private val numRows = 2 * 1000 * 1000
  private val numIters = 3

  private val ruleName = GeneratorNestedFieldPruning.ruleName

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
    spark.read.parquet(s"$dir/orders").createOrReplaceTempView("orders")
  }

  private def runCase(name: String, query: String): Unit = {
    val benchmark = new Benchmark(name, numRows, numIters, output = output)
    benchmark.addCase("without GeneratorNestedFieldPruning") { _ =>
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
        spark.sql(query).noop()
      }
    }
    benchmark.addCase("with GeneratorNestedFieldPruning") { _ =>
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
      }
    }
  }
}
