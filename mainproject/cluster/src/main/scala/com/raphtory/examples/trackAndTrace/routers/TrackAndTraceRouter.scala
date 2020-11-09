package com.raphtory.examples.trackAndTrace.routers

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class TrackAndTraceRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int, override val initialRouterCount: Int)
  extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount, initialRouterCount) {
  val EARTH_EQU = 6378137.0                                                          //m
  val EARTH_POL = 6356752.3142                                                       //m
  val STEPSIZE  = System.getenv().getOrDefault("MAP_GRID_SIZE", "100").trim.toDouble //m

  override protected def parseTuple(tuple: StringSpoutGoing): List[GraphUpdate] = {
    val datapoint  = lineToDatapoint(tuple.value.split(",").map(_.trim))
    val eventTime  = datapoint.time
    val userID     = datapoint.userId
    val latitude   = datapoint.latitude
    val longitude  = datapoint.longitude
    val locationID = locationIDGenerator(latitude, longitude)
    val commands = new ListBuffer[GraphUpdate]()
    commands+=(VertexAdd(eventTime, userID, Type("User")))

    commands+=(
            VertexAddWithProperties(
                    eventTime,
                    locationID,
                    Properties(DoubleProperty("latitude", latitude), DoubleProperty("longitude", longitude)),
                    Type("Location")
            )
    )

    commands+=(EdgeAdd(eventTime, userID, locationID, Type("User Visted Location")))
    commands.toList
  }

  //converts the line into a case class which has all of the data via the correct name and type
  def lineToDatapoint(line: Array[String]): Datapoint =
    Datapoint(
            line(0).toLong,                    //User ID
            line(1).toLong,                    //Trip ID
            line(2).toLong,                    //OSM way ID
            line(3).toLong,                    //OSM node ID
            line(4).toDouble,                  //Latitude
            line(5).toDouble,                  //Longitude
            line(6),                           //Node type
            line(7),                           //Road classification
            line(8),                           //Rail road classification
            line(9),                           //Water way classification
            line(10),                          //Aerial way classification
            line(11).toLong * 1000,            //Location time, in seconds  (milli for raphtory)
            line(15).toLong,                   //Location duration
            line(16).toDouble,                 //Location speed
            line(17).toDouble,                 //	Location distance
            line(18).toDouble,                 //Location accuracy
            line(19),                          //Location quality
            line(20),                          //Location modality
            longCheck(line(21)),               //SM relation ID
            line(22).toDouble,                 //O	Altitude in m
            (line(23).toDouble * 1000).toLong, //Time estimate in decimal seconds (converted to millis)
            line(24)                           //	Source
    )

  def longCheck(data: String): Option[Long] = if (data equals "") None else Some(data.toLong)

  def getCartCoord(lat: Double, long: Double): (Double, Double) = {
    val e = 1 - (Math.pow(EARTH_EQU, 2) / Math.pow(EARTH_POL, 2))
    val N = EARTH_EQU / (Math.sqrt(1 - (e * Math.pow(Math.sin(lat), 2))))
    val x = N * Math.cos(lat) * Math.cos(long)
    val y = N * Math.cos(lat) * Math.sin(long)
    (x, y)
  }

  def locationIDGenerator(latitude: Double, longitude: Double): Long = { //make a unique id for the location
    val (x, y) = getCartCoord(latitude, longitude) // converts lat-long to cartesian coordinates
    val ptx    = Math.floor(x / STEPSIZE) * STEPSIZE // gets coordinates of grid s.t. (x,y) in grid
    val pty    = Math.floor(y / STEPSIZE) * STEPSIZE
    assignID(ptx.toString + pty.toString) //assigns unique ID to grid
  }
}

case class Datapoint(
    userId: Long,           //User ID
    tripId: Long,           //Trip ID
    wayId: Long,            //OSM way ID
    nodeId: Long,           //OSM node ID
    latitude: Double,       //Latitude
    longitude: Double,      //Longitude
    nodeType: String,       //Node type
    highway: String,        //Road classification
    railway: String,        //Rail road classification
    waterway: String,       //Water way classification
    aerialway: String,      //Aerial way classification
    time: Long,             //Location time, in seconds  (milli for raphtory)
    duration: Long,         //Location duration
    speed: Double,          //Location speed
    distance: Double,       //	Location distance
    deviation: Double,      //Location accuracy
    quality: String,        //Location quality
    modality: String,       //Location modality from accelerometer; when quality=missing this indicates the modality of the used corridor or filled-in segment in a sensing gap
    relation: Option[Long], //SM relation ID //wrapped as an option because data missing
    altitude: Double,       //O	Altitude in m
    estimatedTime: Long,    //Time estimate in decimal seconds
    source: String          //	Where this location originates from, either map matching (M) or sensing (S)
)
