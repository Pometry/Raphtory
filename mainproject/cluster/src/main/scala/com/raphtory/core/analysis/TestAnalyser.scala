package com.raphtory.core.analysis

import com.raphtory.core.storage.controller.GraphRepoProxy

class TestAnalyser extends Analyser {
  override def analyse(proxy: GraphRepoProxy.type) = "hello"
}
