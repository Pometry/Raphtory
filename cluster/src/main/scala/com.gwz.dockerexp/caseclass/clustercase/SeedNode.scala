package com.gwz.dockerexp.caseclass.clustercase

import com.gwz.dockerexp.Actors.ClusterActors.DocSvr
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry

case class SeedNode() extends DocSvr {
  implicit val system = init(List(ipAndPort.hostIP + ":" + ipAndPort.akkaPort))
}

