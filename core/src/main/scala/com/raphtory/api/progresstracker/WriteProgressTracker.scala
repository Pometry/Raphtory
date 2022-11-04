package com.raphtory.api.progresstracker

import com.raphtory.api.time.Perspective

import scala.concurrent.duration.Duration

case class WriteProgressTracker(jobID: String, perspective: Perspective) extends ProgressTracker(jobID) {
  latestPerspective = Some(perspective)
  perspectivesList.addOne(perspective)
  perspectivesDurations.addOne(0)
  jobDone = true
}
