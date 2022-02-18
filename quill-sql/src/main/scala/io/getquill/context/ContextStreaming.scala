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

trait ContextPrepareFunction[Dialect <: Idiom, Naming <: NamingStrategy] extends ContextPrepare[Dialect, Naming]:
  self: Context[Dialect, Naming] =>

  type PrepareQueryResult = Session => Result[PrepareRow]
  type PrepareActionResult = Session => Result[PrepareRow]
  type PrepareBatchActionResult = Session => Result[List[PrepareRow]]
end ContextPrepareFunction

trait ContextStreaming[Dialect <: io.getquill.idiom.Idiom, Naming <: NamingStrategy] extends ProtoStreamContext[Dialect, Naming]:
  self: Context[Dialect, Naming] =>

  @targetName("streamQuery")
  inline def stream[T](inline quoted: Quoted[Query[T]]): StreamResult[T] = {
    val ca = new ContextOperation[Nothing, T, Dialect, Naming, PrepareRow, ResultRow, Session, this.type, StreamResult[T]](self.idiom, self.naming) {
      def execute(sql: String, prepare: (PrepareRow, Session) => (List[Any], PrepareRow), extraction: Extraction[ResultRow, Session, T], executionInfo: ExecutionInfo, fetchSize: Option[Int]) =
        val extract = extraction match
          case Extraction.Simple(extract) => extract
          case _ => throw new IllegalArgumentException("Extractor required")

        val runContext = DatasourceContextInjectionMacro[RunnerBehavior, Runner, this.type](context)
        self.streamQuery(fetchSize, sql, prepare, extract)(executionInfo, runContext)
    }
    // TODO Could make Quoted operation constructor that is a typeclass, not really necessary though
    QueryExecution.apply(quoted, ca, None)
  }

  @targetName("streamQueryWithFetchSize")
  inline def stream[T](inline quoted: Quoted[Query[T]], fetchSize: Int): StreamResult[T] = {
    val ca = new ContextOperation[Nothing, T, Dialect, Naming, PrepareRow, ResultRow, Session, this.type, StreamResult[T]](self.idiom, self.naming) {
      def execute(sql: String, prepare: (PrepareRow, Session) => (List[Any], PrepareRow), extraction: Extraction[ResultRow, Session, T], executionInfo: ExecutionInfo, fetchSize: Option[Int]) =
        val extract = extraction match
          case Extraction.Simple(extract) => extract
          case _ => throw new IllegalArgumentException("Extractor required")

        val runContext = DatasourceContextInjectionMacro[RunnerBehavior, Runner, this.type](context)
        self.streamQuery(fetchSize, sql, prepare, extract)(executionInfo, runContext)
    }
    // TODO Could make Quoted operation constructor that is a typeclass, not really necessary though
    QueryExecution.apply(quoted, ca, Some(fetchSize))
  }
end ContextStreaming