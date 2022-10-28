package com.raphtory.api.progresstracker

import com.raphtory.api.time.Perspective
import com.raphtory.internals.components.querymanager.QueryManagement

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

abstract class ProgressTracker(jobID: String) {

  var jobDone: Boolean                          = false
  var latestPerspective: Option[Perspective]    = None
  val perspectivesList: ListBuffer[Perspective] = new ListBuffer[Perspective]()
  val perspectivesDurations: ListBuffer[Long]   = new ListBuffer[Long]()

  /** Returns job identifier for the query
    * @return job identifier
    */
  final def getJobId: String = jobID

  /** Returns the latest `Perspective` processed by the query
    * @return latest perspective
    */
  final def getLatestPerspectiveProcessed: Option[Perspective] = latestPerspective

  /** Returns list of perspectives processed for the query so far
    * @return a list of perspectives
    */
  final def getPerspectivesProcessed: List[Perspective] = perspectivesList.toList

  /** Returns the time taken to process each perspective in milliseconds */
  final def getPerspectiveDurations: List[Long] = perspectivesDurations.toList

  /** Checks if job is complete
    * @return job status
    */
  def isJobDone: Boolean = jobDone

  def handleMessage(msg: QueryManagement): Unit = {}
}
