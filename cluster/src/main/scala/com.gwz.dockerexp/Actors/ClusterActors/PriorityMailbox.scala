package com.gwz.dockerexp.Actors.ClusterActors

import akka.actor.ActorSystem
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import com.gwz.dockerexp.caseclass.BenchmarkUpdate
import com.typesafe.config.Config
/**
  * Created by Mirate on 14/08/2017.
  */
// We inherit, in this case, from UnboundedPriorityMailbox
// and seed it with the priority generator
class PriorityMailbox(settings: ActorSystem.Settings, config: Config) extends UnboundedPriorityMailbox(
  PriorityGenerator {
    case BenchmarkUpdate => 0
    //case otherwise     => 1
    case _ => 1
  })