package com.streamingkmeans

import com.clustering.KMeans.SelectNearestCenter
import com.streamingkmeans.utils.EuclideanDistanceMeasure
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.math.{BLAS, DenseVector}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._

class KMeansPlus (
//               var k: Int,
               var maxIterations: Int,
               var centroids:DataSet[DenseVector]
               ) extends Serializable {
  def train( points: DataSet[DenseVector]): DataSet[DenseVector] ={
    val finalCentroids:DataSet[DenseVector] = centroids.iterate(maxIterations) { currentCentroids =>
      val newCentroids = points
          .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")

        .map { x => (x._1, x._2, 1L) }
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1,
          {
            BLAS.axpy(1,p1._2,p2._2) //y += a*x
            p2._2
          }
          , p1._3 + p2._3) }
        .map { x => {
          BLAS.scal(  1.0 / x._3 ,x._2 ) // x= a*x
          x._2
        }}
      newCentroids
    }
    centroids = finalCentroids
    finalCentroids

    //法2 使用while进行迭代
//    var iteration = 0
//    var currentCentroids = centroids
//    while(iteration<maxIterations){
//      currentCentroids.print()
//      currentCentroids = points
//        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
    //        .map { x => (x._1, x._2, 1L) }
//        .groupBy(0)
//        .reduce { (p1, p2) => (p1._1,
//          {
//            BLAS.axpy(1,p1._2,p2._2) //y += a*x
//            p2._2
//          }
//          , p1._3 + p2._3) }
//        .map { x => {
//          BLAS.scal(  1.0 / x._3 ,x._2 ) // x= a*x
//          x._2
//        }}
//      currentCentroids.print()
//      iteration+=1
//    }
  }
  def predict(points:DataSet[DenseVector]): DataSet[(Int,DenseVector)] ={
      val result:DataSet[(Int,DenseVector)] = points
        .map(new SelectNearestCenter)
        .withBroadcastSet(centroids, "centroids")
      result
  }
  def predict(points:DataStream[DenseVector],center:Array[DenseVector]): DataStream[(Int,DenseVector)] ={
      val result:DataStream[(Int,DenseVector)] = points.map(point=>{
        val re = EuclideanDistanceMeasure.findClosest(point,center)
        (re._1,point)
      }).setParallelism(1)
    result
  }
}

final class SelectNearestCenter extends RichMapFunction[DenseVector, (Int, DenseVector)] with Serializable{
  private var centroids: Traversable[DenseVector] = null
  override def open(parameters: Configuration) {
    centroids = getRuntimeContext.getBroadcastVariable[DenseVector]("centroids").asScala
  }
  def map(p: DenseVector): (Int, DenseVector) = {
    var minDistance: Double = Double.MaxValue
    var closestCentroidId: Int = -1
//    for (centroid <- centroids) {
//      val distance = EuclideanDistanceMeasure.distance(p,centroid)
//      if (distance < minDistance) {
//        minDistance = distance
//        closestCentroidId = centroid.id
//      }
//    }
    //返回的是（中心下表，点信息）
    for(i <- 0 to centroids.size-1){
      val arr = centroids.toArray
      val distance = EuclideanDistanceMeasure.distance(p,arr(i))
      if (distance < minDistance) {
        minDistance = distance
        closestCentroidId = i
      }
    }
    (closestCentroidId, p)
  }
}