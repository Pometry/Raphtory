package com.raphtory.examples.trackAndTrace.routers


import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._


class TrackAndTraceRouter(routerId: Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    val datapoint = lineToDatapoint(record.asInstanceOf[String].split(",").map(_.trim))
    val eventTime = datapoint.time
    val userID = datapoint.userId
    val latitude = doubleTruncator(datapoint.latitude)
    val longitude = doubleTruncator(datapoint.longitude)
    val locationID = locationIDGenerator(latitude,longitude)

    sendGraphUpdate(VertexAdd(eventTime, userID, Type("User")))

    sendGraphUpdate(VertexAddWithProperties(eventTime, locationID,
      Properties(DoubleProperty("latitude",latitude),DoubleProperty("longitude",longitude)),
      Type("Location")))

    sendGraphUpdate(EdgeAdd(eventTime, userID, locationID, Type("User Visted Location")))

  }

  //converts the line into a case class which has all of the data via the correct name and type
  def lineToDatapoint(line: Array[String]) = {
     Datapoint(line(0).toLong,//User ID
      line(1).toLong, //Trip ID
      line(2).toLong, //OSM way ID
      line(3).toLong, //OSM node ID
      line(4).toDouble, //Latitude
      line(5).toDouble,//Longitude
      line(6), //Node type
      line(7), //Road classification
      line(8), //Rail road classification
      line(9), //Water way classification
      line(10), //Aerial way classification
      line(11).toLong*1000,  //Location time, in seconds  (milli for raphtory)
      line(15).toLong, //Location duration
      line(16).toDouble,  //Location speed
      line(17).toDouble,  //	Location distance
      line(18).toDouble, //Location accuracy
      line(19), //Location quality
      line(20),  //Location modality
      line(21).toLong, //SM relation ID
      line(22).toDouble, //O	Altitude in m
      (line(23).toDouble*1000).toLong, //Time estimate in decimal seconds (converted to millis)
      line(24),//	Source
      )
  }
 def doubleTruncator(latlong:Double) = { //todo decide how many points to shave off
   latlong
 }

 def locationIDGenerator(latitude:Double,longitude:Double): Long = { //make a unique id for the location
   assignID(latitude.toString+longitude.toString)
 }
}

case class Datapoint(userId:Long, //User ID
                     tripId:Long, //Trip ID
                     wayId:Long, //OSM way ID
                     nodeId:Long, //OSM node ID
                     latitude:Double, //Latitude
                     longitude:Double, //Longitude
                     nodeType:String, //Node type
                     highway:String, //Road classification
                     railway:String, //Rail road classification
                     waterway:String, //Water way classification
                     aerialway:String, //Aerial way classification
                     time:Long, //Location time, in seconds  (milli for raphtory)
                     duration:Long, //Location duration
                     speed:Double, //Location speed
                     distance:Double, //	Location distance
                     deviation:Double, //Location accuracy
                     quality:String, //Location quality
                     modality:String, //Location modality from accelerometer; when quality=missing this indicates the modality of the used corridor or filled-in segment in a sensing gap
                     relation:Long, //SM relation ID
                     altitude:Double, //O	Altitude in m
                     estimatedTime:Long, //Time estimate in decimal seconds
                     source:String //	Where this location originates from, either map matching (M) or sensing (S)
                    )
