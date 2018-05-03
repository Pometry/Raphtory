package com.raphtory.core.analysis

import com.raphtory.core.storage.controller.GraphRepoProxy

trait Analyser extends java.io.Serializable{
  def analyse(proxy : GraphRepoProxy.type ) : Any
}
