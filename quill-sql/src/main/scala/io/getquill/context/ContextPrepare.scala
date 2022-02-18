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

trait ContextPrepare[Dialect <: Idiom, Naming <: NamingStrategy]:
  self: Context[Dialect, Naming] =>

  type Result[T]
  type Session
  type Runner

  type PrepareQueryResult //Usually: Session => Result[PrepareRow]
  type PrepareActionResult //Usually: Session => Result[PrepareRow]
  type PrepareBatchActionResult //Usually: Session => Result[List[PrepareRow]]

  def prepareQuery(sql: String, prepare: Prepare = identityPrepare)(executionInfo: ExecutionInfo, dc: Runner): PrepareQueryResult
  def prepareSingle(sql: String, prepare: Prepare = identityPrepare)(executionInfo: ExecutionInfo, dc: Runner): PrepareQueryResult
  def prepareAction(sql: String, prepare: Prepare = identityPrepare)(executionInfo: ExecutionInfo, dc: Runner): PrepareActionResult
  def prepareBatchAction(groups: List[BatchGroup])(executionInfo: ExecutionInfo, dc: Runner): PrepareBatchActionResult

  @targetName("runPrepareQuery")
  inline def prepare[T](inline quoted: Quoted[Query[T]]): PrepareQueryResult = {
    val ca = new ContextOperation[Nothing, T, Dialect, Naming, PrepareRow, ResultRow, Session, this.type, PrepareQueryResult](self.idiom, self.naming) {
      def execute(sql: String, prepare: (PrepareRow, Session) => (List[Any], PrepareRow), extraction: Extraction[ResultRow, Session, T], executionInfo: ExecutionInfo, fetchSize: Option[Int]) =
        val extract = extraction match
          case Extraction.Simple(extract) => extract
          case _ => throw new IllegalArgumentException("Extractor required")

        val runContext = DatasourceContextInjectionMacro[RunnerBehavior, Runner, this.type](context)
        self.prepareQuery(sql, prepare)(executionInfo, runContext)
    }
    QueryExecution.apply(quoted, ca, None)
  }

  @targetName("runPrepareQuerySingle")
  inline def prepare[T](inline quoted: Quoted[T]): PrepareQueryResult = prepare(QuerySingleAsQuery(quoted))

  @targetName("runPrepareAction")
  inline def prepare[E](inline quoted: Quoted[Action[E]]): PrepareActionResult = {
    val ca = new ContextOperation[E, Any, Dialect, Naming, PrepareRow, ResultRow, Session, this.type, PrepareActionResult](self.idiom, self.naming) {
      def execute(sql: String, prepare: (PrepareRow, Session) => (List[Any], PrepareRow), extraction: Extraction[ResultRow, Session, Any], executionInfo: ExecutionInfo, fetchSize: Option[Int]) =
        val runContext = DatasourceContextInjectionMacro[RunnerBehavior, Runner, this.type](context)
        self.prepareAction(sql, prepare)(executionInfo, runContext)
    }
    QueryExecution.apply(quoted, ca, None)
  }

  @targetName("runPrepareBatchAction")
  inline def prepare[I, A <: Action[I] & QAC[I, Nothing]](inline quoted: Quoted[BatchAction[A]]): PrepareBatchActionResult = {
    val ca = new BatchContextOperation[I, Nothing, A, Dialect, Naming, PrepareRow, ResultRow, Session, PrepareBatchActionResult](self.idiom, self.naming) {
      def execute(sql: String, prepares: List[(PrepareRow, Session) => (List[Any], PrepareRow)], extraction: Extraction[ResultRow, Session, Nothing], executionInfo: ExecutionInfo) =
        val runContext = DatasourceContextInjectionMacro[RunnerBehavior, Runner, this.type](context)
        val group = BatchGroup(sql, prepares)
        self.prepareBatchAction(List(group))(executionInfo, runContext)
    }
    BatchQueryExecution.apply(quoted, ca)
  }
end ContextPrepare