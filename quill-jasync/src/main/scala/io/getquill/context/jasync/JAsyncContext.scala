package io.getquill.context.jasync

import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.{ ConcreteConnection, Connection, QueryResult }
import io.getquill.context.sql.idiom.SqlIdiom
//import io.getquill.monad.ScalaFutureIOMonad
import io.getquill.util.ContextLogger
import io.getquill.{ NamingStrategy, ReturnAction }
import kotlin.jvm.functions.Function1

import java.util.concurrent.CompletableFuture
import scala.compat.java8.FutureConverters
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try
import io.getquill.context.ExecutionInfo
import io.getquill.context.RunnerSummoningBehavior

abstract class JAsyncContext[D <: SqlIdiom, N <: NamingStrategy, C <: ConcreteConnection](val idiom: D, val naming: N, pool: ConnectionPool[C])
  extends JAsyncContextBase[D, N]
  //with ScalaFutureIOMonad
{

  private val logger = ContextLogger(classOf[JAsyncContext[_, _, _]])

  override type Result[T] = Future[T]
  override type RunQueryResult[T] = Seq[T]
  override type RunQuerySingleResult[T] = T
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = Seq[Long]
  override type RunBatchActionReturningResult[T] = Seq[T]

  implicit def toFuture[T](cf: CompletableFuture[T]): Future[T] = FutureConverters.toScala(cf)
  implicit def toCompletableFuture[T](f: Future[T]): CompletableFuture[T] = FutureConverters.toJava(f).asInstanceOf[CompletableFuture[T]]
  implicit def toKotlinFunction[T, R](f: T => R): Function1[T, R] = new Function1[T, R] {
    override def invoke(t: T): R = f(t)
  }

  override def close = {
    Await.result(pool.disconnect(), Duration.Inf)
    ()
  }

  protected def withConnection[T](f: Connection => Future[T])(implicit ec: ExecutionContext) =
    ec match {
      case TransactionalExecutionContext(ec, conn) => f(conn)
      case other                                   => f(pool)
    }

  protected def extractActionResult[O](returningAction: ReturnAction, extractor: Extractor[O])(result: QueryResult): O

  protected def expandAction(sql: String, returningAction: ReturnAction) = sql

  def probe(sql: String) =
    Try {
      Await.result(pool.sendQuery(sql), Duration.Inf)
    }

  def transaction[T](f: TransactionalExecutionContext => Future[T])(implicit ec: ExecutionContext) =
    ec match {
      case c: TransactionalExecutionContext => toCompletableFuture(f(c))
      case _ => pool.inTransaction({ (c: Connection) /* Need to add parens here for Dotty */ =>
        toCompletableFuture(f(TransactionalExecutionContext(ec, c)))
      })
    }

  // Quill IO Monad not in Protoquill yet
  // override def performIO[T](io: IO[T, _], transactional: Boolean = false)(implicit ec: ExecutionContext): Result[T] =
  //   transactional match {
  //     case false => super.performIO(io)
  //     case true  => transaction(super.performIO(io)(_))
  //   }

  // TODO Remove from all contexts
  override def context: Runner = throw new IllegalStateException("Runner (ExecutionContext) of JAsyncContext is summoned implicitly, the member is unused.")

  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(executionInfo: ExecutionInfo, dc: ExecutionContext): Future[List[T]] = {
    implicit val ec = dc // implicitly define the execution context that will be passed in
    val (params, values) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withConnection(_.sendPreparedStatement(sql, values.asJava))
      .map(_.getRows.asScala.iterator.map(row => extractor(row, ())).toList)
  }

  def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor)(executionInfo: ExecutionInfo, dc: ExecutionContext): Future[T] =
    implicit val ec = dc
    executeQuery(sql, prepare, extractor)(executionInfo, dc).map(handleSingleResult)

  def executeAction(sql: String, prepare: Prepare = identityPrepare)(executionInfo: ExecutionInfo, dc: ExecutionContext): Future[Long] = {
    implicit val ec = dc // implicitly define the execution context that will be passed in
    val (params, values) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withConnection(_.sendPreparedStatement(sql, values.asJava)).map(_.getRowsAffected)
  }

  def executeActionReturning[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T], returningAction: ReturnAction)(executionInfo: ExecutionInfo, dc: ExecutionContext): Future[T] = {
    implicit val ec = dc // implicitly define the execution context that will be passed in
    val expanded = expandAction(sql, returningAction)
    val (params, values) = prepare(Nil, ())
    logger.logQuery(sql, params)
    withConnection(_.sendPreparedStatement(expanded, values.asJava))
      .map(extractActionResult(returningAction, extractor))
  }

  def executeBatchAction(groups: List[BatchGroup])(executionInfo: ExecutionInfo, dc: ExecutionContext): Future[List[Long]] =
    implicit val ec = dc // implicitly define the execution context that will be passed in
    Future.sequence {
      groups.map {
        case BatchGroup(sql, prepare) =>
          prepare.foldLeft(Future.successful(List.newBuilder[Long])) {
            case (acc, prepare) =>
              acc.flatMap { list =>
                executeAction(sql, prepare)(executionInfo, dc).map(list += _)
              }
          }.map(_.result())
      }
    }.map(_.flatten.toList)

  def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T])(executionInfo: ExecutionInfo, dc: ExecutionContext): Future[List[T]] =
    implicit val ec = dc // implicitly define the execution context that will be passed in
    Future.sequence {
      groups.map {
        case BatchGroupReturning(sql, column, prepare) =>
          prepare.foldLeft(Future.successful(List.newBuilder[T])) {
            case (acc, prepare) =>
              acc.flatMap { list =>
                executeActionReturning(sql, prepare, extractor, column)(executionInfo, dc).map(list += _)
              }
          }.map(_.result())
      }
    }.map(_.flatten.toList)

  // Used for TranslateContext. The functionality of context.translate is not in Protoquill yet.
  //override private[getquill] def prepareParams(statement: String, prepare: Prepare): Seq[String] =
  //  prepare(Nil)._2.map(prepareParam)

}