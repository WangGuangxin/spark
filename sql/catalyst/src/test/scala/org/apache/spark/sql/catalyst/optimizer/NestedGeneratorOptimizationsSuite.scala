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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Tests for [[GeneratorNestedFieldPruning]].
 */
class NestedGeneratorOptimizationsSuite extends SchemaPruningTest {

  private val item = StructType.fromDDL("a int, b string, c double")
  private val rel = LocalRelation($"k".int, $"arr".array(item))

  private def exploded(outer: Boolean = false): LogicalPlan =
    rel.generate(Explode($"arr"), outer = outer, outputNames = Seq("g"))

  private def g(field: String): Expression = $"g".getField(field)

  private def generates(plan: LogicalPlan): Seq[Generate] =
    plan.collect { case g: Generate => g }

  private def extractedFields(e: Expression): Set[String] =
    e.collect { case f: GetArrayStructFields => f.field.name }.toSet

  private def checkResolvedWithSameSchema(optimized: LogicalPlan, query: LogicalPlan): Unit = {
    assert(optimized.resolved, s"Not resolved:\n$optimized")
    assert(optimized.schema === query.schema)
  }

  // ---------------------------------------------------------------------------------------------
  // GeneratorNestedFieldPruning
  // ---------------------------------------------------------------------------------------------

  test("GeneratorNestedFieldPruning: several used fields are zipped") {
    val query = exploded()
      .where(g("a") > 1)
      .select($"k", g("b").as("b"))
      .analyze
    val optimized = GeneratorNestedFieldPruning(query)
    val gen = generates(optimized).head.generator
    assert(gen.children.head.isInstanceOf[ArraysZip])
    assert(extractedFields(gen) === Set("a", "b"))
    checkResolvedWithSameSchema(optimized, query)
    // Idempotent.
    comparePlans(GeneratorNestedFieldPruning(optimized), optimized)
  }

  test("GeneratorNestedFieldPruning: zipped fields are extracted as flat columns") {
    val query = exploded()
      .where(g("a") > 1)
      .select($"k", g("b").as("b"))
      .analyze
    val optimized = GeneratorNestedFieldPruning(query)
    // Right above the generate, a Project turns the zipped struct into top-level columns ...
    val extract = optimized.collectFirst { case p @ Project(_, _: Generate) => p }
    assert(extract.isDefined, s"No Project above the generate:\n$optimized")
    assert(extract.get.output.forall(!_.dataType.isInstanceOf[StructType]))
    // ... and nothing above it reads a struct any more.
    optimized.foreach {
      case f: Filter => assert(f.condition.collect { case s: GetStructField => s }.isEmpty)
      case p: Project if !p.child.isInstanceOf[Generate] =>
        assert(p.projectList.flatMap(_.collect { case s: GetStructField => s }).isEmpty)
      case _ =>
    }
  }

  test("GeneratorNestedFieldPruning: a single used field is exploded directly") {
    val query = exploded()
      .where(g("a") > 1)
      .select($"k", g("a").as("a"))
      .analyze
    val optimized = GeneratorNestedFieldPruning(query)
    val gen = generates(optimized).head.generator
    assert(gen.children.head.isInstanceOf[GetArrayStructFields])
    assert(extractedFields(gen) === Set("a"))
    checkResolvedWithSameSchema(optimized, query)
    comparePlans(GeneratorNestedFieldPruning(optimized), optimized)
  }

  test("GeneratorNestedFieldPruning: posexplode and outer generates") {
    Seq(false, true).foreach { outer =>
      val query = rel.generate(PosExplode($"arr"), outer = outer, outputNames = Seq("pos", "g"))
        .where(g("a") > 1)
        .select($"pos", g("b").as("b"), g("c").as("c"))
        .analyze
      val optimized = GeneratorNestedFieldPruning(query)
      val gen = generates(optimized).head
      assert(gen.generator.isInstanceOf[PosExplode])
      assert(gen.outer === outer)
      assert(extractedFields(gen.generator) === Set("a", "b", "c"))
      checkResolvedWithSameSchema(optimized, query)
    }
  }

  test("GeneratorNestedFieldPruning: the struct may be forwarded through a Project") {
    val query = exploded()
      .select($"k", $"g")
      .where(g("a") > 1)
      .select($"k", g("c").as("c"))
      .analyze
    val optimized = GeneratorNestedFieldPruning(query)
    assert(extractedFields(generates(optimized).head.generator) === Set("a", "c"))
    checkResolvedWithSameSchema(optimized, query)
  }

  test("GeneratorNestedFieldPruning: no change when the whole struct is used") {
    val inOutput = exploded()
      .where(g("a") > 1)
      .select($"k", $"g")
      .analyze
    comparePlans(GeneratorNestedFieldPruning(inOutput), inOutput)

    val inExpression = exploded()
      .where(g("a") > 1 && $"g".isNotNull)
      .select($"k", g("b").as("b"))
      .analyze
    comparePlans(GeneratorNestedFieldPruning(inExpression), inExpression)
  }

  test("GeneratorNestedFieldPruning: no change when every field is used") {
    val query = exploded()
      .select(g("a").as("a"), g("b").as("b"), g("c").as("c"))
      .analyze
    comparePlans(GeneratorNestedFieldPruning(query), query)
  }

  test("GeneratorNestedFieldPruning: no change when the struct passes a non-transparent node") {
    val branch = exploded().select($"g")
    val query = branch.union(branch).select($"g".getField("a").as("a")).analyze
    comparePlans(GeneratorNestedFieldPruning(query), query)
  }

  test("GeneratorNestedFieldPruning: disabled with nested schema pruning") {
    withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
      val query = exploded()
        .where(g("a") > 1)
        .select($"k", g("b").as("b"))
        .analyze
      comparePlans(GeneratorNestedFieldPruning(query), query)
    }
  }
}
