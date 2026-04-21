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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Rewrite rollup Expand + (partial/final) HashAggregate into staged prefix aggregations
 * to avoid expanding raw rows.
 *
 * v1 limitations:
 * - Only HashAggregateExec (partial + final) patterns
 * - No DISTINCT aggregates
 * - No aggregate FILTER clauses
 * - Only strict prefix-rollup Expand pattern with gid encoding matching rollup
 */
object OptimizeRollupPrefixAgg extends Rule[SparkPlan] {

  private case class RollupLevel(prefixLen: Int, gidValue: Long)
  private case class ParsedRollup(levels: Seq[RollupLevel])

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.rollupPrefixAggEnabled) return plan

    plan.transformUp {
      case finalAgg: HashAggregateExec =>
        rewriteIfMatches(finalAgg).getOrElse(finalAgg)
    }
  }

  private def rewriteIfMatches(finalAgg: HashAggregateExec): Option[SparkPlan] = {
    val (exchange, partialAgg) = finalAgg.child match {
      case e: ShuffleExchangeExec =>
        e.child match {
          case p: HashAggregateExec => (e, p)
          case _ => return None
        }
      case _ => return None
    }

    val expand = partialAgg.child match {
      case ex: ExpandExec => ex
      case _ => return None
    }

    if (!noDistinctNoFilter(partialAgg.aggregateExpressions) ||
      !noDistinctNoFilter(finalAgg.aggregateExpressions)) {
      return None
    }

    val (gidAttr, groupKeyAttrs) =
      splitGroupingId(partialAgg.groupingExpressions, expand.output).getOrElse(return None)

    val minKeys = SQLConf.get.rollupPrefixAggMinKeys
    if (groupKeyAttrs.length < minKeys) return None

    val rollup = parseStrictRollupExpand(expand, groupKeyAttrs, gidAttr).getOrElse(return None)

    // We will build level-by-level, chaining each level's FINAL output as next level's input.
    val baseChild = expand.child
    
    // Map groupKeyAttrs (from Expand output) back to baseChild's output attributes
    val baseChildKeys = groupKeyAttrs.map { expandKey =>
      // Find the corresponding attribute in baseChild.output by matching name and dataType
      baseChild.output.find(attr =>
        attr.name == expandKey.name && attr.dataType == expandKey.dataType
      ).getOrElse(expandKey) // fallback to expandKey if not found
    }
    
    val levelPlans = buildLevels(
      baseChild = baseChild,
      originalFinalAgg = finalAgg,
      originalPartialAgg = partialAgg,
      originalExchange = exchange,
      rollup = rollup,
      allKeys = groupKeyAttrs,
      baseChildKeys = baseChildKeys
    )

    // Union levels, then project to the original finalAgg resultExpressions to preserve output
    // ordering/exprIds expected by the rest of the plan.
    val union = UnionExec(levelPlans)
    val projected = ProjectExec(finalAgg.resultExpressions, union)
    Some(projected)
  }

  private def noDistinctNoFilter(aggs: Seq[AggregateExpression]): Boolean =
    aggs.forall(ae => !ae.isDistinct && ae.filter.isEmpty)

  private def splitGroupingId(
                               groupingExprs: Seq[NamedExpression],
                               expandOutput: Seq[Attribute]): Option[(Attribute, Seq[Attribute])] = {
    val attrs = groupingExprs.collect { case a: Attribute => a }
    if (attrs.length != groupingExprs.length) return None

    // Prefer attribute that looks like grouping id
    val gidOpt = attrs.find(a =>
      a.dataType == LongType && a.name.toLowerCase(java.util.Locale.ROOT).contains("grouping_id")
    ).orElse {
      // fallback: last attribute in grouping keys is often gid
      attrs.lastOption.filter(_.dataType == LongType)
    }

    gidOpt.map { gid =>
      val keys = attrs.filterNot(_ == gid)
      (gid, keys)
    }
  }

  private def parseStrictRollupExpand(
                                       expand: ExpandExec,
                                       groupKeys: Seq[Attribute],
                                       gid: Attribute): Option[ParsedRollup] = {

    val out = expand.output
    val keyIdx = groupKeys.map(k => out.indexWhere(_.exprId == k.exprId))
    val gidIdx = out.indexWhere(_.exprId == gid.exprId)
    if (keyIdx.exists(_ < 0) || gidIdx < 0) return None

    def isNullLiteral(e: Expression): Boolean = e match {
      case Literal(null, _) => true
      case _ => false
    }
    def longLiteral(e: Expression): Option[Long] = e match {
      case Literal(v: Long, LongType) => Some(v)
      case Literal(v: Int, IntegerType) => Some(v.toLong)
      case _ => None
    }

    val proj = expand.projections
    if (proj.length != groupKeys.length + 1) return None

    val levels = proj.map { p =>
      val keysExprs = keyIdx.map(p)
      val gidLit = longLiteral(p(gidIdx)).getOrElse(return None)

      val prefixLen = keysExprs.takeWhile(e => !isNullLiteral(e)).length
      val suffixOk = keysExprs.drop(prefixLen).forall(isNullLiteral)
      if (!suffixOk) return None

      RollupLevel(prefixLen, gidLit)
    }

    val expectedLens = (groupKeys.length to 0 by -1).toSeq
    if (levels.map(_.prefixLen) != expectedLens) return None

    // Validate gid encoding matches rollup: gid = (1L << (n - prefixLen)) - 1
    val n = groupKeys.length
    val gidOk = levels.forall { l =>
      val expectedGid = (1L << (n - l.prefixLen)) - 1L
      l.gidValue == expectedGid
    }
    if (!gidOk) return None

    Some(ParsedRollup(levels))
  }

  private def buildLevels(
                           baseChild: SparkPlan,
                           originalFinalAgg: HashAggregateExec,
                           originalPartialAgg: HashAggregateExec,
                           originalExchange: ShuffleExchangeExec,
                           rollup: ParsedRollup,
                           allKeys: Seq[Attribute],
                           baseChildKeys: Seq[Attribute]): Seq[SparkPlan] = {

    val oldPartialKeyCount = originalPartialAgg.groupingExpressions.length
    val oldFinalKeyCount = originalFinalAgg.groupingExpressions.length

    def mkLevel(input: SparkPlan, prefixLen: Int, inputKeys: Seq[Attribute]): SparkPlan = {
      // Use the input's key attributes, not the original allKeys
      val prefixKeys: Seq[NamedExpression] = inputKeys.take(prefixLen)

      // Build partial aggregate for this level by copying original partial aggregate shape.
      val newPartial = originalPartialAgg.copy(
        groupingExpressions = prefixKeys,
        resultExpressions =
          prefixKeys.map(_.toAttribute) ++ originalPartialAgg.resultExpressions.drop(oldPartialKeyCount),
        child = input)

      // Shuffle like original
      val newShuffle = originalExchange.copy(child = newPartial)

      // Final aggregate for this level
      val newFinal = originalFinalAgg.copy(
        groupingExpressions = prefixKeys,
        resultExpressions =
          prefixKeys.map(_.toAttribute) ++ originalFinalAgg.resultExpressions.drop(oldFinalKeyCount),
        child = newShuffle)

      // Expand-free rollup needs to output full key list with null suffix.
      val finalOut = newFinal.output
      val producedPrefix = finalOut.take(prefixLen)

      val nullSuffix: Seq[NamedExpression] =
        allKeys.drop(prefixLen).map { a =>
          // Keep the same name/qualifier as original key attr; exprId will be normalized by outer ProjectExec.
          Alias(Literal(null, a.dataType), a.name)(qualifier = a.qualifier)
        }

      val aggOut = finalOut.drop(prefixLen)
      ProjectExec(producedPrefix ++ nullSuffix ++ aggOut, newFinal)
    }

    // Chain levels: level n reads baseChild; next reads previous level's output
    var cur: SparkPlan = baseChild
    var currentKeys: Seq[Attribute] = baseChildKeys  // Start with baseChild's attributes
    val byPrefixLen = rollup.levels.map(_.prefixLen) // n, n-1, ..., 0
    byPrefixLen.map { k =>
      val p = mkLevel(cur, k, currentKeys)
      cur = p
      // Update currentKeys to be the key attributes from the current level's output
      currentKeys = p.output.take(allKeys.length).map(_.toAttribute)
      p
    }
  }
}