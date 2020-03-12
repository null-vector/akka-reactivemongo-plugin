package org

import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

package object nullvector {

  implicit def futureWriteResult2Try(futureResult: Future[WriteResult])(implicit ec: ExecutionContext): Future[Try[Unit]] = {
    futureResult.map(result =>
      if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
    )
  }

  implicit def futureBulkWriteResult2Try(futureResult: Future[MultiBulkWriteResult])(implicit ec: ExecutionContext): Future[Try[Unit]] = {
    futureResult.map(result =>
      if (result.ok) Success({}) else Failure(new Exception(result.writeErrors.map(_.toString).mkString("\n")))
    )
  }

}
