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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.MapType

/**
 * When comparing two maps, we have to make sure two maps have the same key value pairs but
 * with different key ordering are equal.
 * For example, Map('a' -> 1, 'b' -> 2) equals to Map('b' -> 2, 'a' -> 1).
 *
 * We have to specially handle this in grouping/join/window because Spark SQL turns
 * grouping/join/window partition keys into binary `UnsafeRow` and compare the
 * binary data directly instead of using MapType's ordering. So in these cases, we have
 * to insert an expression to sort map entries by key.
 *
 * Note that, this rule must be executed at the end of optimizer, because the optimizer may create
 * new joins(the subquery rewrite) and new join conditions(the join reorder).
 */
object NormalizeMapType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case cmp @ BinaryComparison(left, right) if containsUnorderedMap(left) =>
      cmp.withNewChildren(SortMapKeys(left) :: right :: Nil)
    case cmp @ BinaryComparison(left, right) if containsUnorderedMap(right) =>
      cmp.withNewChildren(left :: SortMapKeys(right) :: Nil)
    case sort: SortOrder if containsUnorderedMap(sort.child) =>
      sort.copy(child = SortMapKeys(sort.child))
  } transform {
    case w: Window if w.partitionSpec.exists(p => containsUnorderedMap(p)) =>
      w.copy(partitionSpec = w.partitionSpec.map(normalize))

    case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _, _)
      // The analyzer guarantees left and right joins keys are of the same data type.
      if leftKeys.exists(k => containsUnorderedMap(k)) =>
      val newLeftJoinKeys = leftKeys.map(normalize)
      val newRightJoinKeys = rightKeys.map(normalize)
      val newConditions = newLeftJoinKeys.zip(newRightJoinKeys).map {
        case (l, r) => EqualTo(l, r)
      } ++ condition
      j.copy(condition = Some(newConditions.reduce(And)))
  }

  /**
   * Check if a dataType contains an unordered map.
   */
  private def containsUnorderedMap(e: Expression): Boolean = {
    e.dataType.existsRecursively {
      case _: MapType => true
      case _ => false
    }
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case e if containsUnorderedMap(e) => SortMapKeys(e)
    case o => o
  }
}
