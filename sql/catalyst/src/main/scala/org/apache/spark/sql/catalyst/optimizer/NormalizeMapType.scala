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

import scala.math.Ordering
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.{getValue, javaType}
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, BinaryComparison, EqualTo, ExpectsInputTypes, Expression, SortOrder, UnaryExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, MapData, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, DataType, MapType}

object NormalizeMapType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case cmp @ BinaryComparison(left, right) if containsUnorderedMap(left) =>
      cmp.withNewChildren(ReorderMapKey(left) :: right :: Nil)
    case cmp @ BinaryComparison(left, right) if containsUnorderedMap(right) =>
      cmp.withNewChildren(left :: ReorderMapKey(right) :: Nil)
    case sort: SortOrder if containsUnorderedMap(sort.child) =>
      sort.copy(child = ReorderMapKey(sort.child))
  } transform {
    case a: Aggregate if a.groupingExpressions.exists(containsUnorderedMap) =>
      // Modify the top level grouping expressions
      val replacements = a.groupingExpressions.collect {
        case a: Attribute if containsUnorderedMap(a) =>
          a -> Alias(ReorderMapKey(a), a.name)(exprId = a.exprId, qualifier = a.qualifier)
        case e if containsUnorderedMap(e) =>
          e -> ReorderMapKey(e)
      }

      // Tranform the expression tree.
      a.transformExpressionsUp {
        case e =>
          replacements
            .find(_._1.semanticEquals(e))
            .map(_._2)
            .getOrElse(e)
      }

    case w: Window if w.partitionSpec.exists(p => needNormalize(p)) =>
      w.copy(partitionSpec = w.partitionSpec.map(normalize))

    case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _, _)
      // The analyzer guarantees left and right joins keys are of the same data type.
      if leftKeys.exists(k => needNormalize(k)) =>
      val newLeftJoinKeys = leftKeys.map(normalize)
      val newRightJoinKeys = rightKeys.map(normalize)
      val newConditions = newLeftJoinKeys.zip(newRightJoinKeys).map {
        case (l, r) => EqualTo(l, r)
      } ++ condition
      j.copy(condition = Some(newConditions.reduce(And)))
  }

  private def containsUnorderedMap(e: Expression): Boolean =
    MapType.containsUnorderedMap(e.dataType)

  private def needNormalize(expr: Expression): Boolean = expr match {
    case ReorderMapKey(_) => false
    case e if e.dataType.isInstanceOf[MapType] => true
    case _ => false
  }

  private[sql] def normalize(expr: Expression): Expression = expr match {
    case _ if !needNormalize(expr) => expr
    case e if e.dataType.isInstanceOf[MapType] =>
      ReorderMapKey(e)
  }
}

case class ReorderMapKey(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  private val MapType(keyType, valueType, valueContainsNull, _) = dataType.asInstanceOf[MapType]
  private lazy val keyOrdering: Ordering[Any] = TypeUtils.getInterpretedOrdering(keyType)
  private lazy val mapBuilder = new ArrayBasedMapBuilder(keyType, valueType)

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = new MapType(keyType, valueType, valueContainsNull, true)

  override def nullSafeEval(input: Any): Any = {
    val childMap = input.asInstanceOf[MapData]
    val childMapKey = childMap.keyArray()
    val childMapValue = childMap.valueArray()
    val sortedKeyIndex = (0 until childMap.numElements()).toArray.sorted(new Ordering[Int] {
      override def compare(a: Int, b: Int): Int = {
        keyOrdering.compare(childMapKey.get(a, keyType), childMapKey.get(b, keyType))
      }
    })

    var i = 0
    while (i < childMap.numElements()) {
      val index = sortedKeyIndex(i)
      mapBuilder.put(
        childMapKey.get(index, keyType),
        childMapValue.get(index, valueType))

      i += 1
    }

    mapBuilder.build()
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val initIndexArrayFunc = ctx.freshName("initIndexArray")
    val numElements = ctx.freshName("numElements")
    val sortedKeyIndex = ctx.freshName("sortedKeyIndex")
    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")
    val idx = ctx.freshName("idx")
    val builderTerm = ctx.addReferenceObj("mapBuilder", mapBuilder)
    ctx.addNewFunction(initIndexArrayFunc,
      s"""
         |private Integer[] $initIndexArrayFunc(int n) {
         |  Integer[] arr = new Integer[n];
         |  for (int i = 0; i < n; i++) {
         |    arr[i] = i;
         |  }
         |  return arr;
         |}""".stripMargin)

    val codeToNormalize = (f: String) => {
      s"""
         |int $numElements = $f.numElements();
         |Integer[] $sortedKeyIndex = $initIndexArrayFunc($numElements);
         |final ArrayData $keyArray = $f.keyArray();
         |final ArrayData $valueArray = $f.valueArray();
         |java.util.Arrays.sort($sortedKeyIndex, new java.util.Comparator<Integer>() {
         |   @Override
         |   public int compare(Object a, Object b) {
         |     Integer indexA = (Integer)a;
         |     Integer indexB = (Integer)b;
         |     ${javaType(keyType)} keyA = ${getValue(keyArray, keyType, "indexA")};
         |     ${javaType(keyType)} keyB = ${getValue(keyArray, keyType, "indexB")};
         |     return ${ctx.genComp(keyType, "keyA", "keyB")};
         |   }
         |});
         |
         |for (int $idx = 0; $idx < $numElements; $idx++) {
         |  Integer index = $sortedKeyIndex[$idx];
         |  $builderTerm.put(
         |    ${getValue(keyArray, keyType, "index")},
         |    ${getValue(valueArray, valueType, "index")});
         |}
         |
         |${ev.value} = $builderTerm.build();
         |""".stripMargin
    }

    nullSafeCodeGen(ctx, ev, codeToNormalize)
  }
}