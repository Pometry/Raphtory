package com

import com.oblac.nomen.Nomen

package object raphtory {
  private[raphtory] def createName: String = Nomen.est().adjective().color().animal().get()
}
