package com.raphtory

import cats.Applicative

package object protocol {
  def success: Status                       = Status(success = true)
  def failure: Status                       = Status(success = false)
  def success[F[_]: Applicative]: F[Status] = Applicative[F].pure(success)
  def failure[F[_]: Applicative]: F[Status] = Applicative[F].pure(failure)
}
