package com.streamingkmeans

import com.streamingkmeans.utils.{EuclideanDistanceMeasure, StringToDense}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

class StreamingKMeans(
                       var k:Int,
                       var dim:Int, // 点的维度
                       var decayFactor:Double,
                       var clusterCenters:Array[DenseVector],
                       var clusterWeights: Array[Double]
                     ) extends Serializable{
  def this() = this(3,2,1,null,null)
  def setK(k:Int):this.type = {
    this.k = k
    this
  }
  def setDecayFactor(decayFactor: Double): this.type = {
    this.decayFactor = decayFactor
    this
  }
  def setHalfLife(halfLife: Double): this.type = {
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    this
  }
  def setInitialCenters(centers: Array[DenseVector], weights: Array[Double]): this.type = {
    this.clusterCenters = centers
    this.clusterWeights = weights
    this
  }
  //预留
  def setRandomCenters(){
  }
    //卡住了
  def trainOnBatch(data:DataStream[String]):Unit={
    val dense:DataStream[DenseVector] = data.map(new StringToDense)

    /**
     * apply:将给定的窗口函数应用于每个窗口。
     * 针对每个key分别调用窗口的每个计算值，调用窗口函数。
     * 窗口函数的输出被解释为常规的非窗口流。
     * 这个函数并不要求在计算窗口之前缓冲窗口中的所有数据，因为该函数不提供预聚合的方法。
     */
    val re = dense.countWindowAll(4)
      .apply{( window: GlobalWindow,
               events: Iterable[DenseVector],
               out: Collector[StreamingKMeans]) =>
        out.collect(updateBactch(events))
      }
  }

  def predictOn(data:DataStream[List[Double]]): Unit ={
  }
  //卡住了
  def updateBactch(data:Iterable[DenseVector]): StreamingKMeans ={
    //    if(clusterCenters==null) clusterCenters = getCentroidDataSet()
    //找到最近的簇中心
    //    val closest = data.map( p => (this.predict(p),(p,1L)))
    //    data.print()
    System.out.println(data)
//    val re:Iterable[(Int,Double)] = data.map(EuclideanDistanceMeasure.findClosest(_,clusterCenters))
    val bp1:Iterable[(Int,(DenseVector,Int))] = data.map(dv=>{
      val fdc:(Int,Double) = EuclideanDistanceMeasure.findClosest(dv,clusterCenters)
      (fdc._1,(dv,1)) //(bestIndex,(Point,1))
    })
      val re:Map[Int,Iterable[(Int,(DenseVector,Int))]] = bp1.groupBy(_._1)//这里下不去了groupBy返回的是Map
    System.out.println(re)
//    System.out.print(re)
    null
  }

  def trainOnStream(data:DataStream[String]):Unit={
    val dense:DataStream[DenseVector] = data.map(new StringToDense)
//    dense.print()
    val result = updateStream(dense)
//    result.print()
  }
  //来一条更新一条
  def updateStream(data:DataStream[DenseVector]): DataStream[(Int,DenseVector)] ={
    //初始化一个之前簇中心
//    val arr:Array[Double] = Array.fill[Double](dim)(1.0)
//    val centers:Array[DenseVector] = Array.fill[DenseVector](k)(DenseVector.apply(arr))
//    var index = 0
//    for (center <- clusterCenters) {
//      centers.update(index,center)
//      index += 1
//    }
//    var indexw = 0
//    val weights: Array[Double] = Array.fill[Double](k)(0.0)
//    for(weight <- clusterWeights){
//      weights.update(indexw,weight)
//      indexw += 1
//    }

    val centers = clusterCenters
    val weights = clusterWeights
    val IP1:DataStream[(Int,DenseVector)] = data.map(point=>{
      System.out.println(point)
      val fdc:(Int,Double) = EuclideanDistanceMeasure.findClosest(point,centers)
      val a = (fdc._1,point) //(bestIndex,Point)
//      System.out.println(a._1+"前"+weights(a._1))
      val center:DenseVector = centers(a._1) //获取point最近的簇信息
      val den = weights(a._1)*decayFactor+1.0 //nt+1=nt*a+1 就是分母
      val ant = decayFactor*weights(a._1) // a*nt
      BLAS.axpy(ant,center,a._2) //y += a * x 也就是 ct*nt*a + xt
      BLAS.scal( 1.0/den,a._2) // x = a * x 也就是 a._2 = (ct*nt*a + xt) / (nt*a+1)
      centers.update(a._1,a._2)
      weights.update(a._1,weights(a._1)+1)
//      System.out.println(a._1+"后"+weights(a._1))
      var i = 0
      for (center <- centers) {
        System.out.print("center:"+center)
        clusterCenters.update(i, center)
        i += 1
      }
      System.out.println()
      var iw = 0
      for (center <- weights) {
        System.out.print("weights:"+center)
        clusterWeights.update(iw, center)
        iw += 1
      }
      System.out.println()
      (a._1,a._2)// 返回的就是(id,ct+1)
    }).setParallelism(1)
    IP1
  }

}
