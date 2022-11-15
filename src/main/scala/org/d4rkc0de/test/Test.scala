package org.d4rkc0de.test

import scala.collection.mutable.TreeSet

object Test extends App {
  val segments: TreeSet[Tuple2[Int, Int]] = TreeSet((0, 10), (6, 10), (0, 2), (4, 7))
  segments.foreach(println)
  getChilds((0,10) ,segments)

  def getChilds(segment: Tuple2[Int, Int], segments: TreeSet[Tuple2[Int, Int]]): Unit = {
//    val x = segments.collectFirst({})
    val deb = segments.find(seg => seg._1 >= segment._1)
    val fin = segments.find(seg => seg._2 <= segment._2)
    println(deb + " " + fin)

  }

  def getIntersection(segment: Tuple2[Int, Int], segments: Array[Tuple2[Int, Int]]): Unit = {

  }


}
