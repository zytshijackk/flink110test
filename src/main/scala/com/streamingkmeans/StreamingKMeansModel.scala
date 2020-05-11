package com.streamingkmeans

import java.io.File

import com.streamingkmeans.utils.{EuclideanDistanceMeasure, StringToDense}
import ml.kmeans.LocalKMeans
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.Utils
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.{Collector, XORShiftRandom}



//实现微批模型
class StreamingKMeansModel(
//                       var k:Int,
//                       var dim:Int, // 点的维度
//                       var decayFactor:Double,
                       var clusterCenters:Array[DenseVector],
                       var clusterWeights: Array[Double]
                     ) extends Serializable{
//  var clusterCenters:Array[DenseVector] = clusterCenters1
//  var clusterWeights:Array[Double] = clusterWeights1
  def train(data:DataStream[String],k:Int,dim:Int,decayFactor:Double){
    val dense:DataStream[DenseVector] = data.map(new StringToDense)
    /**
     * apply:将给定的窗口函数应用于每个窗口。
     * 针对每个key分别调用窗口的每个计算值，调用窗口函数。
     * 窗口函数的输出被解释为常规的非窗口流。
     * 这个函数并不要求在计算窗口之前缓冲窗口中的所有数据，因为该函数不提供预聚合的方法。
     */
//    val re: DataStream[Seq[(Int, DenseVector, Int)]] = dense.countWindowAll(4)
//      .apply{( window: GlobalWindow,
//               events: Iterable[DenseVector],
//               out: Collector[Seq[(Int, DenseVector, Int)]]) =>
//        out.collect(update(events,k,dim,decayFactor))
//      }
//    re.print()
//    System.out.println("!!!"+clusterWeights(1))
//    System.out.println("!!!"+clusterCenters(1))
//    System.out.println(re)
  }

//  def predict(points:DataStream[DenseVector]): DataStream[(Int,Double)] ={
//      points.map(EuclideanDistanceMeasure.findClosest(_, clusterCenters))
//  }

  def predict(points:DataSet[DenseVector]): DataSet[(Int,Double,DenseVector,DenseVector)] ={
      points
        .map(new PredictMap)
        .withBroadcastSet(points.getExecutionEnvironment.fromCollection(clusterCenters),"centroids")
  }

  //未测试
  def computeCost(data: DataSet[DenseVector]): DataSet[Double] = {
//    val cost = data.map(p =>
//      EuclideanDistanceMeasure.pointCost(bcCentersWithNorm.value, new VectorWithNorm(p)))
      val cost: DataSet[Double] = data.map(new CostMap)
      .withBroadcastSet(data.getExecutionEnvironment.fromCollection(clusterCenters),"centroids")
      .sum(0)
    cost
  }

  /**
   * Ct+1 = ( Ct * Nt * a + Xt * Mt ) / ( Nt * a + Mt )
   * @param data
   * @return
   */
  def update(data:Iterable[DenseVector],k:Int,dim:Int,decayFactor:Double): List[DenseVector] ={
    if(clusterCenters==null){
      System.out.println("in!:")
      val initdata: Array[SparseVector] = data.map(dense=>dense.toSparseVector).toArray
      val centers: Array[SparseVector] = LocalKMeans.kMeansPlusPlus(Utils.RNG.nextLong(), initdata, Array.fill(data.size)(1.0), k, 2)
      val initcenters = centers.map(center => center.toDenseVector)
      clusterCenters = initcenters
      clusterWeights = Array.fill(k)(0.0)
    }else{
      System.out.println("clusterCenters:"+clusterCenters.toList.toString)
    }
    val cost1 = data
      .map(point=>(EuclideanDistanceMeasure.findClosest(point,clusterCenters)._1,point))
//      .sum
    System.out.println("cost1:"+cost1)
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
//    var iteration = 1
//    while (iteration < 100) {
    System.out.println(currentWeights.toList.toString)
      val result: Seq[(Int, DenseVector, Double)] = windowPoints
        .map(new TestCenter).withBroadcastSet(windowPoints.getExecutionEnvironment.fromCollection(currentCenters), "centroids") //(closestCentroidId, p)
        .withBroadcastSet(windowPoints.getExecutionEnvironment.fromCollection(currentWeights), "weights")
        //      .withBroadcastSet(decayFactor,"")
        .withBroadcastSet(windowPoints.getExecutionEnvironment.fromElements(decayFactor), "decay")
        .groupBy(0)
        .reduce {
          (x, y) => {
            (x._1, {
              BLAS.axpy(1, x._2, y._2) //y += a*x
              y._2
            }, x._3, x._4 + y._4, x._5, x._6)
          }
        } //(bestCenterId,xt*mt,nt,mt,ct1,decay)
        .map {
          x => {
            (
              x._1, {
              BLAS.scal(x._3 * x._6, x._5) //x._5 = nt * ct1
              BLAS.axpy(1.0, x._2, x._5) // x._5 = nt * ct1 + xt * mt
              BLAS.scal(1.0 / (x._3 * x._6 + x._4), x._5)
              x._5
            }, x._4 + x._3
            )
          }
        }.collect()
//      result.foreach {
//        case (j, newCenter, weight) => {
//          System.out.println("index " + j + ":")
//          currentCenters.update(j, newCenter)
//          System.out.println("centers:" + currentCenters(j))
//          currentWeights.update(j, weight)
//          System.out.println("weights:" + currentWeights(j))
//        }
//      }
      for(i <- 0 to result.size-1){
        System.out.println(result(i)._2)
        System.out.println(result(i)._3)
        if(result(i)._2!=null)
        currentCenters.update(i,result(i)._2)
        if(result(i)._3!=null)
        currentWeights.update(i,result(i)._3)
      }
      this.clusterWeights = currentWeights
      this.clusterCenters = currentCenters
    System.out.println("old")
    for(center<-clusterCenters){
      System.out.println(center)
    }
    System.out.println("oldend")
//      iteration += 1
//    }
    //通过在误差平方和SSE内的计算来评估聚类
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val cost: Double = data
                      .map(EuclideanDistanceMeasure.findClosest(_,clusterCenters)._2)
//                      .sum / data.toArray.size
                      .sum
    System.out.println("cost:"+cost)
    currentCenters.toList
//      .print()
//    new StreamingKMeansModel(currentCenters,currentWeights)
//    result.toList.toString()
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
final class CostMap extends RichMapFunction[DenseVector, Double] with Serializable{
  private var centroids: Traversable[DenseVector] = null
  override def open(parameters: Configuration) {
    centroids = getRuntimeContext.getBroadcastVariable[DenseVector]("centroids").asScala
  }
  def map(p: DenseVector): Double = {
    val centerarr: Array[DenseVector] = centroids.toArray
    val (bestIndex, bestDistance) = EuclideanDistanceMeasure.findClosest(p,centerarr)
    bestDistance
  }
}
final class PredictMap extends RichMapFunction[DenseVector, (Int,Double,DenseVector,DenseVector)] with Serializable{
  private var centroids: Traversable[DenseVector] = null
  override def open(parameters: Configuration) {
    centroids = getRuntimeContext.getBroadcastVariable[DenseVector]("centroids").asScala
  }
  def map(p: DenseVector): (Int,Double,DenseVector,DenseVector) = {
    val centerarr: Array[DenseVector] = centroids.toArray
    val (bestIndex, bestDistance) = EuclideanDistanceMeasure.findClosest(p,centerarr)
    (bestIndex, bestDistance,p,centerarr(bestIndex))
  }
}
class StreamingKMeans(
                       var k:Int,
                       var dim:Int, // 点的维度
                       var decayFactor:Double,
                        var windowSize:Int
                     )extends Serializable {
  def this() = this(2,2,0.5,6)
  protected var model: StreamingKMeansModel = new StreamingKMeansModel(null, null)
  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of clusters must be positive but got ${k}")
    this.k = k
    this
  }
  def setWindowSize(windowSize:Int): this.type ={
    this.windowSize = windowSize
    this
  }
  def setDecayFactor(a: Double): this.type = {
    require(a >= 0,
      s"Decay factor must be nonnegative but got ${a}")
    this.decayFactor = a
    this
  }
  def setDim(d:Int): this.type ={
    this.dim = d
    this
  }
  def setInitialCenters(centers: Array[DenseVector], weights: Array[Double]): this.type = {
    model = new StreamingKMeansModel(centers, weights)
    this
  }
  def setRandomCenters(weight:Double,seed:Long=Utils.RNG.nextLong()): this.type ={
    val random = new XORShiftRandom(seed)
    val centers: Array[DenseVector] = Array.fill(k)(DenseVector(Array.fill(dim)(random.nextGaussian())))
    val weights = Array.fill(k)(weight)
    model = new StreamingKMeansModel(centers,weights)
    this
  }
  def latestModel(): StreamingKMeansModel = {
    model
  }

  def trainOn(data:DataStream[String]): Unit ={
    val dense:DataStream[DenseVector] = data.map(new StringToDense)
    dense.print()
    /**
     * apply:将给定的窗口函数应用于每个窗口。
     * 针对每个key分别调用窗口的每个计算值，调用窗口函数。
     * 窗口函数的输出被解释为常规的非窗口流。
     * 这个函数并不要求在计算窗口之前缓冲窗口中的所有数据，因为该函数不提供预聚合的方法。
     */
    val re = dense.countWindowAll(6)
      .apply{( window: GlobalWindow,
               events: Iterable[DenseVector],
               out: Collector[List[DenseVector]]) =>
        out.collect(model.update(events,k,dim,decayFactor))
      }
    val path = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/model.txt"
    val file = new File(path)
    if(file.exists()) {
      file.delete()
      re.writeAsText(path).setParallelism(1)
    }else{
      re.writeAsText(path).setParallelism(1)
    }
//    re.setParallelism(1)
//    re.map(center=>{
//      val arr = Array.fill[Double](dim)(1.0)
//      val weights = Array.fill[Double](dim)(1.0)
//      val centers: Array[DenseVector] =  Array.fill[DenseVector](k)(DenseVector.apply(arr))
//      for(list<-center){
//        centers.update(list._1,list._2)
//        weights.update(list._1,model.clusterWeights(list._1)+list._3)
//      }
//      System.out.println("aa"+model.clusterCenters(1))
//      model = new StreamingKMeansModel(centers,weights)
//      System.out.println("bb"+model.clusterCenters(1))
//    }).setParallelism(1)
  }

  def predictOn(data:DataSet[String]): DataSet[(Int,Double,DenseVector,DenseVector)] ={
    val dense:DataSet[DenseVector] = data.map(new StringToDense)
    model.predict(dense)
  }
}
//object StreamingKMeans2 extends Serializable {
//  def run(): Unit ={
//
//  }
//}