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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.StructType

/**
 * Tests for [[GeneratorArrayPrefilter]].
 */
class GeneratorArrayPrefilterSuite extends PlanTest {

  private val item = StructType.fromDDL("a int, b string, c double")
  private val rel = LocalRelation($"k".int, $"arr".array(item))

  private def exploded(outer: Boolean = false): LogicalPlan =
    rel.generate(Explode($"arr"), outer = outer, outputNames = Seq("g"))

  private def g(field: String): Expression = $"g".getField(field)

  private def generates(plan: LogicalPlan): Seq[Generate] =
    plan.collect { case g: Generate => g }

  private def checkResolvedWithSameSchema(optimized: LogicalPlan, query: LogicalPlan): Unit = {
    assert(optimized.resolved, s"Not resolved:\n$optimized")
    assert(optimized.schema === query.schema)
  }

  private def prefilterOf(plan: LogicalPlan): Option[Expression] =
    generates(plan).head.child match {
      case Filter(cond, _) => Some(cond)
      case _ => None
    }

  test("copies the filter on the generated value to the array") {
    val query = exploded()
      .where(g("a") > 1 && $"k" > 0)
      .analyze
    val optimized = GeneratorArrayPrefilter(query)
    val cond = prefilterOf(optimized)
    assert(cond.exists(_.isInstanceOf[ArrayExists]), s"No prefilter:\n$optimized")
    // Only the conjunct on the generated value is copied.
    assert(!cond.get.references.exists(_.name == "k"))
    // The original filter stays.
    assert(optimized.asInstanceOf[Filter].condition == query.asInstanceOf[Filter].condition)
    checkResolvedWithSameSchema(optimized, query)
    // Handled once.
    comparePlans(GeneratorArrayPrefilter(optimized), optimized)
  }

  test("posexplode") {
    val query = rel.generate(PosExplode($"arr"), outputNames = Seq("pos", "g"))
      .where(g("a") > 1 && $"pos" > 0)
      .analyze
    val optimized = GeneratorArrayPrefilter(query)
    val cond = prefilterOf(optimized)
    assert(cond.exists(_.isInstanceOf[ArrayExists]), s"No prefilter:\n$optimized")
    // `pos` does not exist below the generate, so that conjunct is not copied.
    assert(cond.get.references.map(_.name).toSet === Set("arr"))
    checkResolvedWithSameSchema(optimized, query)
  }

  test("not applied twice when the generate loses its tags") {
    val optimized = GeneratorArrayPrefilter(exploded().where(g("a") > 1).analyze)
    // Other rules may rebuild the generate without copying its tags.
    val untagged = optimized match {
      case f @ Filter(_, gen: Generate) => f.copy(child = gen.copy())
    }
    comparePlans(GeneratorArrayPrefilter(untagged), untagged)
  }

  test("unchanged plans") {
    val outerQuery = exploded(outer = true).where(g("a") > 1).analyze
    comparePlans(GeneratorArrayPrefilter(outerQuery), outerQuery)

    val notNullQuery = exploded().where($"g".isNotNull).analyze
    comparePlans(GeneratorArrayPrefilter(notNullQuery), notNullQuery)

    val noGeneratedValue = exploded().where($"k" > 1).analyze
    comparePlans(GeneratorArrayPrefilter(noGeneratedValue), noGeneratedValue)

    val nonDeterministic = exploded().where(g("a") > Rand(0) * 10).analyze
    comparePlans(GeneratorArrayPrefilter(nonDeterministic), nonDeterministic)
  }
}
