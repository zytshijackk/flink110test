package com.streamingkmeans

import com.streamingkmeans.utils.{EuclideanDistanceMeasure, StringToDense}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.{BLAS, DenseVector}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector


//实现微批模型
class StreamingKMeans2(
                       var k:Int,
                       var dim:Int, // 点的维度
                       var decayFactor:Double,
                       var clusterCenters1:Array[DenseVector],
                       var clusterWeights1: Array[Double]
                     ) extends Serializable{
  var clusterCenters:Array[DenseVector] = clusterCenters1
  var clusterWeights:Array[Double] = clusterWeights1
  def train(data:DataStream[String]){
    val dense:DataStream[DenseVector] = data.map(new StringToDense)
    /**
     * apply:将给定的窗口函数应用于每个窗口。
     * 针对每个key分别调用窗口的每个计算值，调用窗口函数。
     * 窗口函数的输出被解释为常规的非窗口流。
     * 这个函数并不要求在计算窗口之前缓冲窗口中的所有数据，因为该函数不提供预聚合的方法。
     */
    val re: DataStream[Seq[(Int, DenseVector, Int)]] = dense.countWindowAll(4)
      .apply{( window: GlobalWindow,
               events: Iterable[DenseVector],
               out: Collector[Seq[(Int, DenseVector, Int)]]) =>
        out.collect(update(events))
      }
//    re.print()
//    System.out.println("!!!"+clusterWeights(1))
//    System.out.println("!!!"+clusterCenters(1))
//    System.out.println(re)
  }

  def predict(point:DenseVector): Unit ={
  }

  /**
   * Ct+1 = ( Ct * Nt * a + Xt * Mt ) / ( Nt * a + Mt )
   * @param data
   * @return
   */
  def update(data:Iterable[DenseVector]): Seq[(Int, DenseVector, Int)] ={
    System.out.println("new")
    for(center<-clusterCenters){
      System.out.println(center)
    }
    System.out.println("newend")
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(7)
    val windowPoints: DataSet[DenseVector] = env.fromCollection(data)//X
//    val currentCentroids: DataSet[DenseVector] = env.fromCollection(clusterCenters)
//    val currentWeights: DataSet[Double] = env.fromCollection(clusterWeights)
    val arr = Array.fill[Double](dim)(1.0)
    val currentCenters: Array[DenseVector] =  Array.fill[DenseVector](k)(DenseVector.apply(arr))
    var index = 0
    for (center <- clusterCenters) {
      currentCenters.update(index, center)
      index += 1
    }
    val currentWeights = clusterWeights
//    val decayDataset: DataSet[Double] = env.fromElements(decayFactor)
//    windowPoints.print()
    val result: Seq[(Int, DenseVector, Int)] = windowPoints
      .map(new TestCenter).withBroadcastSet(windowPoints.getExecutionEnvironment.fromCollection(currentCenters), "centroids")//(closestCentroidId, p)
      .withBroadcastSet(windowPoints.getExecutionEnvironment.fromCollection(currentWeights),"weights")
//      .withBroadcastSet(decayFactor,"")
      .withBroadcastSet(windowPoints.getExecutionEnvironment.fromElements(decayFactor),"decay")
      .groupBy(0)
      .reduce{
        (x,y)=>{
          (x._1,{
            BLAS.axpy(1,x._2,y._2) //y += a*x
            y._2
          },x._3,x._4+y._4,x._5,x._6)
        }
      }//(bestCenterId,xt*mt,nt,mt,ct1,decay)
      .map{
        x=>{(
          x._1,
          {
            BLAS.scal(x._3*x._6,x._5) //x._5 = nt * ct1
            BLAS.axpy(1.0,x._2,x._5) // x._5 = nt * ct1 + xt * mt
            BLAS.scal(1.0/(x._3*x._6+x._4),x._5)
            x._5
          },x._4
        )}
      }.collect()

    result.foreach{
      case (j,newCenter,weight)=>{
        System.out.println("index "+j+":")
        currentCenters.update(j,newCenter)
        System.out.println("centers:"+currentCenters(j))
        currentWeights.update(j,clusterWeights(j)+weight)
        System.out.println("weights:"+currentWeights(j))
      }
    }
    this.clusterWeights = currentWeights
    this.clusterCenters = currentCenters
    windowPoints.map{
      point => (EuclideanDistanceMeasure.findClosest(point,clusterCenters)._1,point)
    }
      .print()
    result
//    var i = 0
//    for (center <- centers) {
//      this.clusterCenters.update(i, center)
//      System.out.println("cluster:"+clusterCenters(i))
//      i += 1
//    }
//    var j = 0
//    for (weight <- weights) {
//      this.clusterWeights.update(j, weight)
//      System.out.println("weight:"+clusterWeights(j))
//      j += 1
//    }
//    val rearr: Array[(Int, DenseVector, Int)] = result.toArray
//    for(i<- 0 to rearr.size-1){ //更新
//      val ite = rearr(i)//现在遍历到的
//      System.out.println("ite:"+ite)
//      val index = ite._1 //遍历到的下标
//      System.out.println("index:"+index)
//      clusterCenters.update(index,ite._2)
//      System.out.println("cluster:"+clusterCenters(index))
//      clusterWeights(index) += ite._3
//      System.out.println("weight:"+this.clusterWeights(index))
//    }
//    new StreamingKMeans2(k,dim,decayFactor,clusterCenters,clusterWeights)
//    new StreamingKMeans2()
  }




}
import scala.collection.JavaConverters._
final class TestCenter extends RichMapFunction[DenseVector, (Int,DenseVector,Double,Int,DenseVector,Double)] with Serializable{
  private var centroids: Traversable[DenseVector] = null
  private var weights: Traversable[Double] = null
  private var decay: Double = 0.0
  override def open(parameters: Configuration) {
    centroids = getRuntimeContext.getBroadcastVariable[DenseVector]("centroids").asScala
    weights = getRuntimeContext.getBroadcastVariable[Double]("weights").asScala
    decay = getRuntimeContext.getBroadcastVariable[Double]("decay").asScala(0)
  }
  def map(p: DenseVector): (Int,DenseVector,Double,Int,DenseVector,Double) = {
    val centerarr: Array[DenseVector] = centroids.toArray
    val weightarr: Array[Double] = weights.toArray
    val (bestIndex, bestDistance) = EuclideanDistanceMeasure.findClosest(p,centerarr)
    val weight = weightarr(bestIndex)
    val center = centerarr(bestIndex)
    (bestIndex,p,weight,1,center,decay)
  }
}

object StreamingKMeans2 extends Serializable {
  def run(): Unit ={

  }
}