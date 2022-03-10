package com.raphtory.core.components.spout.instance

import com.raphtory.core.components.spout.Spout
import io.github.redouane59.twitter.dto.tweet.Tweet

case class LiveTwitterSpout() extends Spout[Tweet]
