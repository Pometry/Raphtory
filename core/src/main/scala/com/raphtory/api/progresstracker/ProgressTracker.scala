package com.raphtory.api.progresstracker

import com.raphtory.api.time.Perspective
import com.raphtory.protocol.QueryUpdate

import scala.collection.mutable.ListBuffer

abstract class ProgressTracker(jobID: String) {

  protected var jobDone: Boolean                          = false
  protected var latestPerspective: Option[Perspective]    = None
  protected val perspectivesList: ListBuffer[Perspective] = new ListBuffer[Perspective]()

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

  /** Checks if job is complete
    * @return job status
    */
  def isJobDone: Boolean = jobDone

  private[raphtory] def handleQueryUpdate(msg: QueryUpdate): Unit = {}
}
