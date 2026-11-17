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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression,
  NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Moves a `Generate`, together with a filter on its output, from above a chain of left semi /
 * left anti joins to below them:
 * {{{
 *   Project(P, Filter(c, Generate(g, [Project(PL,] SemiAnti(...SemiAnti(X, R1)..., Rn)[)])))
 *   =>
 *   Project(P,
 *     SemiAnti(...SemiAnti(Project(K, Filter(c, Generate(g, Project(PL', X)))), R1)..., Rn))
 * }}}
 * where `PL'` adds the left join keys to `PL` and `K` keeps only what `P` and the join conditions
 * reference. A `Generate` without a filter on top moves the same way (with `P` = its output).
 * Besides the array not being shuffled, the projection `PL` (e.g. `arr.f` from nested field
 * pruning) then runs before the joins, so only the used fields are carried.
 *
 * Correctness: a left semi/anti join outputs each left row zero times or once, decided only by
 * that row's values (the join condition cannot reference the generated columns, which only exist
 * above the joins). `Generate` maps every input row to its own output rows. The two therefore
 * commute, and the deterministic filter `c` (on the generate output) can move with the generate.
 *
 * Why: `EXISTS` / `NOT EXISTS` predicates become semi/anti joins in the `RewriteSubquery` batch,
 * after `PushDownLeftSemiAntiJoin` already pushed the subqueries below the `explode` of a nested
 * table. The whole array (e.g. `lineitem_data` in TPC-H q21) is then shuffled into the joins and
 * only exploded, filtered and dropped afterwards. On a flat schema the same filter runs before the
 * joins. Moving the generate down lets the array be dropped before the shuffle.
 *
 * Runs in the `RewriteSubquery` batch, after the semi/anti joins are created and the Projects
 * above them are collapsed, where `PushDownLeftSemiAntiJoin` (which moves such joins below a
 * generate) no longer runs.
 */
object PushGenerateBelowSemiAntiJoin extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Filtered generates first, so that the filter moves down together with the generate.
    val filtered = plan.transformUpWithSubqueries {
      case p @ Project(pl, Filter(cond, g: Generate)) =>
        rewrite(pl, Some(cond), g).getOrElse(p)
    }
    filtered.transformUpWithSubqueries {
      case g: Generate => rewrite(g.output, None, g).getOrElse(g)
    }
  }

  private def isSemiAnti(j: Join): Boolean = j.joinType match {
    case LeftSemi | LeftAnti => j.condition.forall(_.deterministic)
    case _ => false
  }

  private def rewrite(
      pl: Seq[NamedExpression],
      cond: Option[Expression],
      g: Generate): Option[Project] = {
    if (!g.generator.deterministic) return None
    cond match {
      case Some(c)
          if !c.deterministic || SubqueryExpression.hasSubquery(c) ||
            !c.references.exists(a => g.generatorOutput.exists(_.exprId == a.exprId)) =>
        return None
      case _ =>
    }
    val (inner, top) = g.child match {
      case Project(list, j: Join) => (Some(list), j)
      case j: Join => (None, j)
      case _ => return None
    }
    // Joins from top to bottom along the left side. Projects between them that only select
    // columns (as `ColumnPruning` adds) are dropped: the joins only drop rows, and the Projects
    // built below and on top keep exactly the columns that are needed.
    var joins = List.empty[Join]
    var cur: LogicalPlan = top
    var done = false
    while (!done) cur match {
      case j: Join if isSemiAnti(j) =>
        joins = joins :+ j
        cur = j.left
      case Project(list, j: Join) if joins.nonEmpty && isSemiAnti(j) &&
          list.forall(_.isInstanceOf[Attribute]) =>
        cur = j
      case _ =>
        done = true
    }
    if (joins.isEmpty) return None
    val x = cur
    val leftRefs = AttributeSet(joins.flatMap(_.condition.toSeq.flatMap(_.references)))
      .filter(x.outputSet.contains)
    val newChild = inner match {
      case Some(list) =>
        if (!list.forall(_.references.subsetOf(x.outputSet))) return None
        val have = AttributeSet(list.map(_.toAttribute))
        Project(list ++ leftRefs.toSeq.filterNot(have.contains), x)
      case None => x
    }
    val newGen = g.copy(unrequiredChildIndex = Nil, child = newChild)
    val filtered = cond.map(Filter(_, newGen)).getOrElse(newGen)
    val keep = AttributeSet(pl.flatMap(_.references)) ++ leftRefs
    if (!keep.subsetOf(filtered.outputSet)) return None
    val pruned = Project(filtered.output.filter(keep.contains), filtered)
    val rebuilt = joins.reverse.foldLeft(pruned: LogicalPlan) {
      case (left, j) => j.copy(left = left)
    }
    Some(Project(pl, rebuilt))
  }
}
