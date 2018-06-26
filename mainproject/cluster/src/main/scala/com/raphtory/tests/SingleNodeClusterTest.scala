package com.raphtory.tests

import com.raphtory.Go._
import com.raphtory.core.clustersetup._
import com.raphtory.core.clustersetup.singlenode.SingleNodeSetup

object SingleNodeClusterTest extends App {

  SingleNodeSetup("192.168.1.92:1600")

}
