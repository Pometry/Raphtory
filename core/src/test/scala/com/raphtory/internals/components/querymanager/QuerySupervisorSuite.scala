package com.raphtory.internals.components.querymanager

import cats.effect._
import com.raphtory.protocol
import com.raphtory.protocol.PartitionService
import fs2.Stream
import munit.CatsEffectSuite
import org.mockito.MockitoSugar.mock

import scala.concurrent.duration._

class QuerySupervisorSuite extends CatsEffectSuite {

  private val qs = ResourceFixture(
          for {
            partitionService <- Resource.eval(Async[IO].delay(mock[PartitionService[IO]]))
            paritions        <- Resource.eval(Async[IO].pure(Map[Int, PartitionService[IO]](0 -> partitionService)))
            supervisor       <- QuerySupervisor[IO](
                                        "testGraphId",
                                        mock[com.typesafe.config.Config],
                                        paritions
                                )
          } yield supervisor
  )

  qs.test(
          "Start ingestion request should start ingestion and add the request to in-progress requests list if both in-progress requests list and pending requests blocking queue are empty"
  ) { supervisor =>
    import supervisor._

    for {
      et1  <- earliestTime.get
      lt1  <- latestTime.get
      ipr1 <- inprogressReqs.get
      _    <- Async[IO].delay {
                assert {
                  ipr1.isEmpty &&
                  pendingReqs.isEmpty &&
                  et1 == Long.MaxValue &&
                  lt1 == Long.MinValue
                }
              }
      _    <- startBlockingIngestion(0)
      et2  <- earliestTime.get
      lt2  <- latestTime.get
      ipr2 <- inprogressReqs.get
      _    <- Async[IO].delay {
                assert {
                  ipr2.size == 1 &&
                  ipr2.head.asInstanceOf[LoadRequest].sourceID == 0 &&
                  pendingReqs.isEmpty &&
                  et2 == Long.MaxValue &&
                  lt2 == Long.MinValue
                }
              }
    } yield ()
  }

  qs.test(
          "Start ingestion request should start ingestion and add the request to in-progress requests list if there are load requests already in in-progress list and that the pending requests blocking queue is empty"
  ) { supervisor =>
    import supervisor._

    for {
      et1  <- earliestTime.get
      lt1  <- latestTime.get
      defr <- Deferred[IO, Unit]
      _    <- inprogressReqs.update(_ + LoadRequest(0, defr))
      _    <- Async[IO].delay {
                assert {
                  pendingReqs.isEmpty &&
                  et1 == Long.MaxValue &&
                  lt1 == Long.MinValue
                }
              }
      _    <- startBlockingIngestion(1)
      et2  <- earliestTime.get
      lt2  <- latestTime.get
      ipr  <- inprogressReqs.get
      _    <- Async[IO].delay {
                assert {
                  ipr.size == 2 &&
                  ipr.last.asInstanceOf[LoadRequest].sourceID == 1 &&
                  pendingReqs.isEmpty &&
                  et2 == Long.MaxValue &&
                  lt2 == Long.MinValue
                }
              }
    } yield ()
  }

  qs.test(
          "Start ingestion request should block ingestion and add the request to pending requests blocking queue if there are load requests already in-progress list and that the pending requests blocking queue is non-empty i.e., it has query requests at the top of the queue"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      lrDefr <- Deferred[IO, Unit]
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + LoadRequest(0, lrDefr))
      _      <- Async[IO].delay(pendingReqs.add(QueryRequest("testQuery", qrDefr)))
      _      <- Async[IO].delay {
                  assert {
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      fib    <- startBlockingIngestion(1).start
      _      <- IO.sleep(50.millis)
      _      <- pendingReqs.toArray.last.asInstanceOf[LoadRequest].release.complete(())
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr    <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr.size == 1 &&
                    ipr.last.asInstanceOf[LoadRequest].sourceID == 0 &&
                    pendingReqs.size == 2 &&
                    pendingReqs.toArray.last.asInstanceOf[LoadRequest].sourceID == 1 &&
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
      _      <- fib.join
    } yield ()
  }

  qs.test(
          "Start ingestion request should block ingestion and add the request to pending requests blocking queue if there are query requests already in-progress and that the pending requests blocking queue is empty"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + QueryRequest("testQuery", qrDefr))
      _      <- Async[IO].delay {
                  assert {
                    pendingReqs.isEmpty &&
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      fib    <- startBlockingIngestion(0).start
      _      <- IO.sleep(50.millis)
      _      <- pendingReqs.toArray.last.asInstanceOf[LoadRequest].release.complete(())
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr    <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr.size == 1 &&
                    ipr.last.asInstanceOf[QueryRequest].queryName == "testQuery" &&
                    pendingReqs.size == 1 &&
                    pendingReqs.toArray.last.asInstanceOf[LoadRequest].sourceID == 0 &&
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
      _      <- fib.join
    } yield ()
  }

  qs.test(
          "Start ingestion request should block ingestion and add the request to pending requests blocking queue if there are query requests already in-progress and that the pending requests blocking queue is non-empty i.e., it has load requests at the top of the queue"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      lrDefr <- Deferred[IO, Unit]
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + QueryRequest("testQuery", qrDefr))
      _      <- Async[IO].delay(pendingReqs.add(LoadRequest(0, lrDefr)))
      _      <- Async[IO].delay {
                  assert {
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      fib    <- startBlockingIngestion(1).start
      _      <- IO.sleep(50.millis)
      _      <- pendingReqs.toArray.last.asInstanceOf[LoadRequest].release.complete(())
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr    <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr.size == 1 &&
                    ipr.last.asInstanceOf[QueryRequest].queryName == "testQuery" &&
                    pendingReqs.size == 2 &&
                    pendingReqs.toArray.last.asInstanceOf[LoadRequest].sourceID == 1 &&
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
      _      <- fib.join
    } yield ()
  }

  qs.test(
          "Ingestion ended intimation should update earliest and latest time, remove pertaining load request from in-progress requests list"
  ) { supervisor =>
    import supervisor._

    for {
      et1     <- earliestTime.get
      lt1     <- latestTime.get
      lrDefr1 <- Deferred[IO, Unit]
      lrDefr2 <- Deferred[IO, Unit]
      _       <- inprogressReqs.update(_ + LoadRequest(0, lrDefr1))
      ipr0    <- inprogressReqs.updateAndGet(_ + LoadRequest(1, lrDefr2))
      _       <- Async[IO].delay {
                   assert {
                     ipr0.size == 2 &&
                     pendingReqs.isEmpty &&
                     et1 == Long.MaxValue &&
                     lt1 == Long.MinValue
                   }
                 }
      _       <- endBlockingIngestion(sourceID = 0, 5, 10)
      _       <- endBlockingIngestion(sourceID = 1, 0, 6)
      et2     <- earliestTime.get
      lt2     <- latestTime.get
      ipr     <- inprogressReqs.get
      _       <- Async[IO].delay {
                   assert {
                     ipr.isEmpty &&
                     pendingReqs.isEmpty &&
                     et2 == 0 &&
                     lt2 == 10
                   }
                 }
    } yield ()
  }

  qs.test(
          "Ingestion ended intimations should release all blocking query requests from the top of the pending requests blocking queue if any present while adding them to the in-progress list"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      lrDefr <- Deferred[IO, Unit]
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + LoadRequest(0, lrDefr))
      _      <- Async[IO].delay(pendingReqs.add(QueryRequest("testQuery", qrDefr)))
      _      <- Async[IO].delay {
                  assert {
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      _      <- endBlockingIngestion(sourceID = 0, 0, 10)
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr    <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr.size == 1 &&
                    ipr.head.asInstanceOf[QueryRequest].queryName == "testQuery" &&
                    pendingReqs.isEmpty &&
                    et2 == 0 &&
                    lt2 == 10
                  }
                }
    } yield ()
  }

  qs.test(
          "Submit query request should delegate query processing to query handler and add the request to in-progress requests list if both in-progress requests list and pending requests blocking queue are empty"
  ) { supervisor =>
    import supervisor._

    for {
      et1  <- earliestTime.get
      lt1  <- latestTime.get
      ipr1 <- inprogressReqs.get
      _    <- Async[IO].delay {
                assert {
                  ipr1.isEmpty &&
                  pendingReqs.isEmpty &&
                  et1 == Long.MaxValue &&
                  lt1 == Long.MinValue
                }
              }
      _    <- processQueryRequest(Query(name = "testQuery", graphID = "1"))
      et2  <- earliestTime.get
      lt2  <- latestTime.get
      ipr2 <- inprogressReqs.get
      _    <- Async[IO].delay {
                assert {
                  ipr2.size == 1 &&
                  ipr2.head.asInstanceOf[QueryRequest].queryName == "testQuery" &&
                  pendingReqs.isEmpty &&
                  et2 == Long.MaxValue &&
                  lt2 == Long.MinValue
                }
              }
    } yield ()
  }

  qs.test(
          "Submit query request should delegate query processing to query handler and add the request to in-progress requests list if there are query requests already in in-progress list and that the pending requests blocking queue is empty"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + QueryRequest("testQuery", qrDefr))
      _      <- Async[IO].delay {
                  assert {
                    pendingReqs.isEmpty &&
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      _      <- processQueryRequest(Query(name = "testQuery2", graphID = "2"))
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr2   <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr2.size == 2 &&
                    ipr2.last.asInstanceOf[QueryRequest].queryName == "testQuery2" &&
                    pendingReqs.isEmpty &&
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
    } yield ()
  }

  qs.test(
          "Submit query request should block query submission and add the request to pending requests blocking queue if there are query requests already in in-progress list and that the pending requests blocking queue is non-empty i.e., it has load requests at the top of the queue"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      lrDefr <- Deferred[IO, Unit]
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + QueryRequest("testQuery", qrDefr))
      _      <- Async[IO].delay(pendingReqs.add(LoadRequest(sourceID = 0, lrDefr)))
      _      <- Async[IO].delay {
                  assert {
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      fib    <- processQueryRequest(Query(name = "testQuery2", graphID = "2")).start
      _      <- IO.sleep(50.millis)
      _      <- pendingReqs.toArray.last.asInstanceOf[QueryRequest].release.complete(())
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr2   <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr2.size == 1 &&
                    ipr2.last.asInstanceOf[QueryRequest].queryName == "testQuery" &&
                    pendingReqs.size() == 2 &&
                    pendingReqs.toArray.last.asInstanceOf[QueryRequest].queryName == "testQuery2"
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
      _      <- fib.join
    } yield ()
  }

  qs.test(
          "Submit query request should block query submission and add the request to pending requests blocking queue if there are load requests already in-progress and that the pending requests blocking queue is empty"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      lrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + LoadRequest(sourceID = 0, lrDefr))
      _      <- Async[IO].delay {
                  assert {
                    pendingReqs.isEmpty &&
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      fib    <- processQueryRequest(Query(name = "testQuery", graphID = "1")).start
      _      <- IO.sleep(50.millis)
      _      <- pendingReqs.toArray.last.asInstanceOf[QueryRequest].release.complete(())
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr    <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr.size == 1 &&
                    ipr.last.asInstanceOf[LoadRequest].sourceID == 0 &&
                    pendingReqs.size() == 1 &&
                    pendingReqs.toArray.last.asInstanceOf[QueryRequest].queryName == "testQuery"
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
      _      <- fib.join
    } yield ()
  }

  qs.test(
          "Submit query request should block query submission and add the request to pending requests blocking queue if there are load requests already in-progress and that the pending requests blocking queue is non-empty i.e., it has query requests at the top of the queue"
  ) { supervisor =>
    import supervisor._

    for {
      et1    <- earliestTime.get
      lt1    <- latestTime.get
      lrDefr <- Deferred[IO, Unit]
      qrDefr <- Deferred[IO, Unit]
      _      <- inprogressReqs.update(_ + LoadRequest(sourceID = 0, lrDefr))
      _      <- Async[IO].delay(pendingReqs.add(QueryRequest("testQuery", qrDefr)))
      _      <- Async[IO].delay {
                  assert {
                    et1 == Long.MaxValue &&
                    lt1 == Long.MinValue
                  }
                }
      fib    <- processQueryRequest(Query(name = "testQuery2", graphID = "2")).start
      _      <- IO.sleep(50.millis)
      _      <- pendingReqs.toArray.last.asInstanceOf[QueryRequest].release.complete(())
      et2    <- earliestTime.get
      lt2    <- latestTime.get
      ipr    <- inprogressReqs.get
      _      <- Async[IO].delay {
                  assert {
                    ipr.size == 1 &&
                    ipr.last.asInstanceOf[LoadRequest].sourceID == 0 &&
                    pendingReqs.size() == 2 &&
                    pendingReqs.toArray.last.asInstanceOf[QueryRequest].queryName == "testQuery2"
                    et2 == Long.MaxValue &&
                    lt2 == Long.MinValue
                  }
                }
      _      <- fib.join
    } yield ()
  }

  qs.test(
          "Query ended intimation should remove pertaining query requests from in-progress requests list, release all blocking load requests from the top of the pending requests blocking queue if any present while adding them to the in-progress list"
  ) { supervisor =>
    import supervisor._

    for {
      et1     <- earliestTime.get
      lt1     <- latestTime.get
      lrDefr  <- Deferred[IO, Unit]
      qrDefr1 <- Deferred[IO, Unit]
      qrDefr2 <- Deferred[IO, Unit]
      _       <- inprogressReqs.update(_ + QueryRequest("testQuery", qrDefr1))
      _       <- inprogressReqs.update(_ + QueryRequest("testQuery2", qrDefr2))
      _       <- Async[IO].delay(pendingReqs.add(LoadRequest(0, lrDefr)))
      _       <- Async[IO].delay {
                   assert {
                     et1 == Long.MaxValue &&
                     lt1 == Long.MinValue
                   }
                 }
      _       <- endQuery(Query(name = "testQuery", graphID = "1"))
      _       <- endQuery(Query(name = "testQuery2", graphID = "2"))
      et2     <- earliestTime.get
      lt2     <- latestTime.get
      ipr     <- inprogressReqs.get
      _       <- Async[IO].delay {
                   assert {
                     ipr.size == 1 &&
                     ipr.last.asInstanceOf[LoadRequest].sourceID == 0 &&
                     pendingReqs.isEmpty &&
                     et2 == Long.MaxValue &&
                     lt2 == Long.MinValue
                   }
                 }
    } yield ()
  }
}
