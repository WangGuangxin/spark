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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/**
 * MapType expressions are not comparable.
 */
object SortMaps extends Rule[LogicalPlan] {
  private def containsUnorderedMap(e: Expression): Boolean =
    e.resolved && MapType.containsUnorderedMap(e.dataType)

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
    case cmp @ BinaryComparison(left, right) if containsUnorderedMap(left) =>
      cmp.withNewChildren(OrderMaps(left) :: OrderMaps(right) :: Nil)
    case sort: SortOrder if containsUnorderedMap(sort.child) =>
      sort.copy(child = OrderMaps(sort.child))
    case s @ SetOperation(left, right) if left.output.exists(containsUnorderedMap) || right.output.exists(containsUnorderedMap) =>
      val newLeft = left()
    case mc: MapConcat if mc.children.exists(containsUnorderedMap) =>
      val newChildren = mc.children.map { a =>
        if (containsUnorderedMap(a)) {
          OrderMaps(a)
        } else {
          a
        }
      }
      mc.withNewChildren(newChildren)
    case mfe: MapFromEntries if containsUnorderedMap(mfe.child) =>
      mfe.copy(child = OrderMaps(mfe.child))
    case cm: CreateMap if cm.children.exists(containsUnorderedMap) =>
      val newChildren = cm.children.map { a =>
        if (containsUnorderedMap(a)) {
          OrderMaps(a)
        } else {
          a
        }
      }
      cm.withNewChildren(newChildren)
    case mfa: MapFromArrays if containsUnorderedMap(mfa.left) =>
      mfa.copy(left = OrderMaps(mfa.left))
    case i @ In(value, list) if containsUnorderedMap(value) =>
      val newList = list.map { a =>
        if (containsUnorderedMap(a)) {
          OrderMaps(a)
        } else {
          a
        }
      }
      i.copy(value = OrderMaps(value), list = newList)

  } resolveOperators {
    case a: Aggregate if a.resolved && a.groupingExpressions.exists(containsUnorderedMap) =>
      // Modify the top level grouping expressions
      val replacements = a.groupingExpressions.collect {
        case a: Attribute if containsUnorderedMap(a) =>
          a -> Alias(OrderMaps(a), a.name)()
        case e if containsUnorderedMap(e) =>
          e -> OrderMaps(e)
      }

      // Tranform the expression tree.
      a.transformExpressionsUp {
        case e =>
          // TODO create an expression map!
          replacements
            .find(_._1.semanticEquals(e))
            .map(_._2)
            .getOrElse(e)
      }

    case Distinct(child) if child.resolved && child.output.exists(containsUnorderedMap) =>
      val projectList = child.output.map { a =>
        if (containsUnorderedMap(a)) {
          Alias(OrderMaps(a), a.name)()
        } else {
          a
        }
      }
      Distinct(Project(projectList, child))

    case w: Window if w.resolved && w.partitionSpec.exists(containsUnorderedMap) =>
      val newPartitionSpec = w.partitionSpec.map { a =>
        if (containsUnorderedMap(a)) {
          OrderMaps(a)
        } else {
          a
        }
      }
      w.copy(partitionSpec = newPartitionSpec)
  }
}
