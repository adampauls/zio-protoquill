package io.getquill.context

import scala.language.higherKinds
import scala.language.experimental.macros
import java.io.Closeable
import scala.compiletime.summonFrom
import scala.util.Try
import io.getquill.{ ReturnAction }
import io.getquill.generic.EncodingDsl
import io.getquill.Quoted
import io.getquill.QueryMeta
import io.getquill.generic._
import io.getquill.context.mirror.MirrorDecoders
import io.getquill.context.mirror.Row
import io.getquill.generic.GenericDecoder
import io.getquill.generic.DecodingType
import io.getquill.Planter
import io.getquill.ast.Ast
import io.getquill.ast.ScalarTag
import scala.quoted._
import io.getquill.idiom.Idiom
import io.getquill.ast.{Transform, QuotationTag}
import io.getquill.QuotationLot
import io.getquill.metaprog.QuotedExpr
import io.getquill.metaprog.PlanterExpr
import io.getquill.idiom.ReifyStatement
import io.getquill.Query
import io.getquill.metaprog.etc.MapFlicer
import io.getquill.util.Messages.fail
import java.io.Closeable
import io.getquill.util.Format
import io.getquill.QAC
import io.getquill.Action
import io.getquill.ActionReturning
import io.getquill.BatchAction
import io.getquill.Literal
import scala.annotation.targetName
import io.getquill.NamingStrategy
import io.getquill.idiom.Idiom
import io.getquill.context.ProtoContext
import io.getquill.context.AstSplicing
import io.getquill.context.RowContext
import io.getquill.metaprog.etc.ColumnsFlicer
import io.getquill.context.Execution.ElaborationBehavior
import io.getquill.OuterSelectWrap
import scala.annotation.tailrec

trait ContextTranslate[Dialect <: Idiom, Naming <: NamingStrategy]
  extends ContextTranslateMacro[Dialect, Naming]:
  self: Context[Dialect, Naming] =>
  override type TranslateResult[T] = T
  override def wrap[T](t: => T): T = t
  override def push[A, B](result: A)(f: A => B): B = f(result)
  override def seq[A](list: List[A]): List[A] = list

trait ContextTranslateProto[Dialect <: Idiom, Naming <: NamingStrategy]:
  self: Context[Dialect, Naming] =>

  type TranslateResult[T]
  type Runner

  def wrap[T](t: => T): TranslateResult[T]
  def push[A, B](result: TranslateResult[A])(f: A => B): TranslateResult[B]
  def seq[A](list: List[TranslateResult[A]]): TranslateResult[List[A]]

  def translateQuery[T](statement: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor, prettyPrint: Boolean = false)(executionInfo: ExecutionInfo, dc: Runner): TranslateResult[String] =
    push(prepareParams(statement, prepare)) { params =>
      val query =
        if (params.nonEmpty) {
          params.foldLeft(statement) {
            case (expanded, param) => expanded.replaceFirst("\\?", param)
          }
        } else {
          statement
        }

      if (prettyPrint)
        idiom.format(query)
      else
        query
    }

  def translateBatchQuery(groups: List[BatchGroup], prettyPrint: Boolean = false)(executionInfo: ExecutionInfo, dc: Runner): TranslateResult[List[String]] =
    seq {
      groups.flatMap { group =>
        group.prepare.map { prepare =>
          translateQuery(group.string, prepare, prettyPrint = prettyPrint)(executionInfo, dc)
        }
      }
    }

  private[getquill] def prepareParams(statement: String, prepare: Prepare): TranslateResult[Seq[String]]

  @tailrec
  final protected def prepareParam(param: Any): String = param match {
    case None | null => "null"
    case Some(x)     => prepareParam(x)
    case str: String => s"'$str'"
    case _           => param.toString
  }
end ContextTranslateProto

trait ContextTranslateMacro[Dialect <: Idiom, Naming <: NamingStrategy] extends ContextTranslateProto[Dialect, Naming]:
  self: Context[Dialect, Naming] =>

  type TranslateResult[T]
  type Runner
  type RunnerBehavior <: RunnerSummoningBehavior

  @targetName("translateQuery")
  inline def translate[T](inline quoted: Quoted[Query[T]], inline prettyPrint: Boolean): TranslateResult[String] = {
    val ca = new ContextOperation[Nothing, T, Dialect, Naming, PrepareRow, ResultRow, Session, this.type, TranslateResult[String]](self.idiom, self.naming) {
      def execute(sql: String, prepare: (PrepareRow, Session) => (List[Any], PrepareRow), extraction: Extraction[ResultRow, Session, T], executionInfo: ExecutionInfo, fetchSize: Option[Int]) =
        val extract = extraction match
          case Extraction.Simple(extract) => extract
          case _ => throw new IllegalArgumentException("Extractor required")

        val runContext = DatasourceContextInjectionMacro[RunnerBehavior, Runner, this.type](context)
        self.translateQuery(sql, prepare, extract)(executionInfo, runContext)
    }
    QueryExecution.apply(quoted, ca, None)
  }

  // def translate[T](quoted: Quoted[T]): TranslateResult[String] = macro QueryMacro.translateQuery[T]
  // def translate[T](quoted: Quoted[Query[T]]): TranslateResult[String] = macro QueryMacro.translateQuery[T]
  // def translate(quoted: Quoted[Action[_]]): TranslateResult[String] = macro ActionMacro.translateQuery
  // def translate(quoted: Quoted[BatchAction[Action[_]]]): TranslateResult[List[String]] = macro ActionMacro.translateBatchQuery

  // def translate[T](quoted: Quoted[T], prettyPrint: Boolean): TranslateResult[String] = macro QueryMacro.translateQueryPrettyPrint[T]
  // def translate[T](quoted: Quoted[Query[T]], prettyPrint: Boolean): TranslateResult[String] = macro QueryMacro.translateQueryPrettyPrint[T]
  // def translate(quoted: Quoted[Action[_]], prettyPrint: Boolean): TranslateResult[String] = macro ActionMacro.translateQueryPrettyPrint
  // def translate(quoted: Quoted[BatchAction[Action[_]]], prettyPrint: Boolean): TranslateResult[List[String]] = macro ActionMacro.translateBatchQueryPrettyPrint

  // def translateQuery[T](statement: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor, prettyPrint: Boolean = false)(executionInfo: ExecutionInfo, dc: Runner): TranslateResult[String]
  // def translateBatchQuery(groups: List[BatchGroup], prettyPrint: Boolean = false)(executionInfo: ExecutionInfo, dc: Runner): TranslateResult[List[String]]
end ContextTranslateMacro