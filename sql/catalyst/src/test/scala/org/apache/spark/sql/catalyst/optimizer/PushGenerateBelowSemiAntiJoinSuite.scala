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
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.StructType

/**
 * Tests for [[PushGenerateBelowSemiAntiJoin]].
 */
class PushGenerateBelowSemiAntiJoinSuite extends PlanTest {

  private val item = StructType.fromDDL("a int, b string, c double")
  private val rel = LocalRelation($"k".int, $"arr".array(item))
  private val other = LocalRelation($"k2".int)
  private val other2 = LocalRelation($"k3".int)

  private def g(field: String): Expression = $"g".getField(field)

  private def generates(plan: LogicalPlan): Seq[Generate] =
    plan.collect { case g: Generate => g }

  private def checkResolvedWithSameSchema(optimized: LogicalPlan, query: LogicalPlan): Unit = {
    assert(optimized.resolved, s"Not resolved:\n$optimized")
    assert(optimized.schema === query.schema)
  }

  private def generateOverJoin(joinType: JoinType, filtered: Boolean): LogicalPlan = {
    val joined = rel.join(other, joinType, Some($"k" === $"k2"))
      .generate(Explode($"arr"), outputNames = Seq("g"))
    val maybeFiltered = if (filtered) joined.where(g("a") > 1) else joined
    maybeFiltered.select(g("b").as("b")).analyze
  }

  /** The generate is below every join, and the filter on its output moved with it. */
  private def checkPushedBelowAll(optimized: LogicalPlan, filtered: Boolean): Unit = {
    optimized.foreach {
      case j: Join => assert(generates(j.left).nonEmpty, s"Generate above a join:\n$optimized")
      case _ =>
    }
    if (filtered) {
      val gen = generates(optimized).head
      assert(optimized.exists {
        case Filter(_, child) => child eq gen
        case _ => false
      }, s"The filter did not move with the generate:\n$optimized")
    }
  }

  Seq(LeftSemi, LeftAnti).foreach { joinType =>
    Seq(true, false).foreach { filtered =>
      test(s"$joinType, filtered = $filtered") {
        val query = generateOverJoin(joinType, filtered)
        val optimized = PushGenerateBelowSemiAntiJoin(query)
        checkPushedBelowAll(optimized, filtered)
        checkResolvedWithSameSchema(optimized, query)
        comparePlans(PushGenerateBelowSemiAntiJoin(optimized), optimized)
      }
    }
  }

  test("a chain of semi and anti joins") {
    val query = rel
      .join(other, LeftSemi, Some($"k" === $"k2"))
      .join(other2, LeftAnti, Some($"k" === $"k3"))
      .generate(Explode($"arr"), outputNames = Seq("g"))
      .where(g("a") > 1)
      .select($"k", g("b").as("b"))
      .analyze
    val optimized = PushGenerateBelowSemiAntiJoin(query)
    checkPushedBelowAll(optimized, filtered = true)
    assert(optimized.collect { case j: Join => j.joinType } === Seq(LeftAnti, LeftSemi))
    checkResolvedWithSameSchema(optimized, query)
  }

  test("a column-only Project between the joins") {
    val query = rel
      .join(other, LeftSemi, Some($"k" === $"k2"))
      .select($"k", $"arr")
      .join(other2, LeftAnti, Some($"k" === $"k3"))
      .generate(Explode($"arr"), outputNames = Seq("g"))
      .where(g("a") > 1)
      .select($"k", g("b").as("b"))
      .analyze
    val optimized = PushGenerateBelowSemiAntiJoin(query)
    checkPushedBelowAll(optimized, filtered = true)
    assert(optimized.collect { case j: Join => j.joinType } === Seq(LeftAnti, LeftSemi))
    checkResolvedWithSameSchema(optimized, query)
  }

  test("a Project between the generate and the joins") {
    // The join key `k` is not in the Project; it is added below the generate for the join.
    val query = rel
      .join(other, LeftSemi, Some($"k" === $"k2"))
      .select($"arr")
      .generate(Explode($"arr"), outputNames = Seq("g"))
      .where(g("a") > 1)
      .select(g("b").as("b"))
      .analyze
    val optimized = PushGenerateBelowSemiAntiJoin(query)
    checkPushedBelowAll(optimized, filtered = true)
    checkResolvedWithSameSchema(optimized, query)
    // The array is not carried into the join.
    val join = optimized.collectFirst { case j: Join => j }.get
    assert(!join.left.output.exists(_.name == "arr"))
  }

  test("unchanged plans") {
    val inner = generateOverJoin(Inner, filtered = true)
    comparePlans(PushGenerateBelowSemiAntiJoin(inner), inner)

    val nonDeterministicGenerator = rel
      .join(other, LeftSemi, Some($"k" === $"k2"))
      .generate(Explode(If(Rand(0) > 0.5, $"arr", $"arr")), outputNames = Seq("g"))
      .select(g("b").as("b"))
      .analyze
    comparePlans(
      PushGenerateBelowSemiAntiJoin(nonDeterministicGenerator), nonDeterministicGenerator)
  }
}
