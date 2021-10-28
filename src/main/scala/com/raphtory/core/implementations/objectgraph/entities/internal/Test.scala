package com.raphtory.core.implementations.objectgraph.entities.internal

import com.raphtory.core.implementations.objectgraph.entities.internal.RaphtoryEdge
import net.openhft.chronicle.map.ChronicleMap

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.Map.Entry
import scala.collection.mutable
import collection.JavaConverters._



object Test extends App {
  case class Edge(id: Int, time: Long, src: Long, dst: Long, property: mutable.TreeMap[Long, Boolean], property2: mutable.Map[String, String])
  val e = Edge(1, 0L, 1L, 2L, mutable.TreeMap.empty, mutable.Map.empty)

  val byteOutputStream = new ByteArrayOutputStream();
  val objectOutputStream = new ObjectOutputStream(byteOutputStream);

  objectOutputStream.writeObject(e);
  objectOutputStream.flush();
  objectOutputStream.close();

  println(byteOutputStream.toByteArray.length);

  val map = ChronicleMap.of(classOf[java.lang.Long], classOf[Edge])
    .entries(50000)
    .constantValueSizeBySample(e)
    .create()


  map.put(1L, e)
  map.put(2L, e)

  val m = map.entrySet().asScala.map { entry =>
    entry.getKey -> entry.getValue
  }.toMap

  println(m)
}
