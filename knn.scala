package org.apache.flink.examples.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala
import org.apache.flink.api.scala._
import org.apache.flink.examples.scala.knn.Point
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.common.operators.Order._
//TODO: Adaptarlo para archivos en lugar de valores fijos
object knn {
  case class Point(label: String, x: Double, y:Double)
  case class NewPoint(label: String, distance: Double)
  case class PointValidate(x: Double, y:Double)

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    val k = 3
    val pointsTrain: scala.DataSet[Point] = env.fromCollection(Array(
      Point("A",1,2),
      Point("A",1,3),
      Point("B",2,4),
      Point("A",1,1),
      Point("A",1,2),
      Point("B",3,4)
    ))

    val pointsClas : scala.DataSet[PointValidate] = env.fromCollection(Array(
      PointValidate(1,2),
      PointValidate(1,3),
      PointValidate(2,4)
      )
    )
    val puntos = pointsClas.collect()
    for (dot <- puntos){
      val train = pointsTrain
      val puntofinal = train.map(x => NewPoint(x.label, math.sqrt(((dot.x-x.x)*(dot.x-x.x)) +((dot.y-x.y)*(dot.y-x.y)))))
          .sortPartition(1,order = ASCENDING)
          .first(k)
          .map(x  => NewPoint(x.label,1))
          .groupBy(0)
          .sum(1)
          .sortPartition(1,DESCENDING)
          .first(1)
      println(puntofinal.collect())

    }

  }

}
