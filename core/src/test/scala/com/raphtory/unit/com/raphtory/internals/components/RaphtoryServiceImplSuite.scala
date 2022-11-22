package com.raphtory.unit.com.raphtory.internals.components

import cats.effect._
import com.raphtory.internals.components._
import com.raphtory.internals.management.id.IDManager
import com.raphtory.protocol._
import munit.CatsEffectSuite
import org.mockito.MockitoSugar._

class RaphtoryServiceImplSuite extends CatsEffectSuite {

  private val clientId  = "testClientId"
  private val graphId   = "testGraphId"
  private val graphInfo = GraphInfo(clientId, graphId)

  private val raphtoryService = ResourceSuiteLocalFixture(
          "RaphtoryServiceFixture",
          for {
            ingestionService <- Resource.eval(Async[IO].delay(mock[IngestionService[IO]]))
            partitionService <- Resource.eval(Async[IO].delay(mock[PartitionService[IO]]))
            queryService     <- Resource.eval(Async[IO].delay(mock[QueryService[IO]]))
            runningGraphs    <- Resource.eval(IO.ref(Map[String, Set[String]]()))
            existingGraphs   <- Resource.eval(IO.ref(Set[String]()))
            service          <- Resource.eval(
                                        Async[IO].delay(
                                                new RaphtoryServiceImpl(
                                                        runningGraphs,
                                                        existingGraphs,
                                                        ingestionService,
                                                        Map[Int, PartitionService[IO]](0 -> partitionService),
                                                        queryService,
                                                        mock[IDManager],
                                                        mock[ServiceRegistry[IO]].topics,
                                                        mock[com.typesafe.config.Config]
                                                )
                                        )
                                )
          } yield {
            when(queryService.establishGraph(graphInfo)).thenAnswer(IO(Status(success = true)))
            when(ingestionService.establishGraph(graphInfo)).thenAnswer(IO(Status(success = true)))
            when(partitionService.establishGraph(graphInfo)).thenAnswer(IO(Status(success = true)))
            when(queryService.destroyGraph(graphInfo)).thenAnswer(IO(Status(success = true)))
            when(ingestionService.destroyGraph(graphInfo)).thenAnswer(IO(Status(success = true)))
            when(partitionService.destroyGraph(graphInfo)).thenAnswer(IO(Status(success = true)))
            service
          }
  )

  override def munitFixtures = List(raphtoryService)

  // Establish Graph
  test("Establishing a graph for a given graph id updates list of graphs maintained by Raphtory service") {
    val service = raphtoryService()
    assertIO(
            service.establishGraph(graphInfo) >>
              service.getGraph(GetGraph("testGraphId")),
            Status(success = true)
    )
  }

  test("Establishing a graph for a given graph id returns with a failed status if graph already found") {
    val service = raphtoryService()
    assertIO(
            service.establishGraph(graphInfo),
            Status()
    )
  }

  // Get Graph
  test(
          "Getting a graph for a given graph id returns with a failed status if no graph was already established for that graph id"
  ) {
    assertIO(
            raphtoryService().getGraph(GetGraph("1testGraphId")),
            Status()
    )
  }

  // Destroy Graph
  test("Destroying graph should rid graph for a given graph id and client id") {
    val service = raphtoryService()
    assertIO(
            service.destroyGraph(DestroyGraph(clientId, graphId)) >>
              service.getGraph(GetGraph(graphId)),
            Status()
    )
  }

  test(
          "Destroying graph with force should rid graph for a given graphId irrespective of any clients working with them"
  ) {
    val service = raphtoryService()
    assertIO(
            service.establishGraph(graphInfo) >>
              service.destroyGraph(DestroyGraph(clientId, graphId, force = true)) >>
              service.getGraph(GetGraph(graphId)),
            Status()
    )
  }

  // Disconnect
  test("Disconnecting client from a graph should remove client id from the list of clients against a graph id") {
    val service = raphtoryService()
    assertIO(
            service.establishGraph(graphInfo) >>
              service.disconnect(graphInfo),
            Status(success = true)
    )
  }

}
