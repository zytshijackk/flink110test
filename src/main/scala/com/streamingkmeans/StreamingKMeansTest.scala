package com.streamingkmeans

import com.streamingkmeans.utils.StringToDense
import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
object StreamingKMeansTest {
  def main(args: Array[String]): Unit = {

    val input = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/point.txt"
    val modelinput = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/model.txt"

    // the port to connect to
    var port = 0
    try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        //System.err.println("No port specified. Please run 'StreamingKMeansTest --port <port>'")
        port  = 9000
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val env2: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val envSet = ExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val socketText: DataStream[String] = env.socketTextStream("127.0.0.1", port, '\n')
//    val text  = env.readTextFile(input)
//    val textSet  = envSet.readTextFile(input)
//    val arr1 = DenseVector(Array[Double](1,1,1)).toSparseVector
//    val arr2 = DenseVector(Array[Double](-1,-1,-1)).toSparseVector
//    val arr = Array[SparseVector](arr1,arr2)
//    val skm = new StreamingKMeans2(2,2,0.6,arr,Array(3.0,2.0))
    val skmeans: StreamingKMeans = new StreamingKMeans()
      .setK(2).setDim(3).setDecayFactor(1)
//        .setInitialCenters(arr,Array(1.0,1.0,1.0))
        .setWindowSize(6)
//        .setRandomCenters(0.0)
    skmeans.trainOn(socketText)
    env.execute()
//    val model: DataStream[String] = env2.readTextFile(modelinput)
//    model.print()
//    env2.execute()

//    Thread.sleep(10000)
//    skmeans.latestModel().predict(textSet.map(new StringToDense)).print()
//    envSet.execute()
//    System.out.println("hahaha"+newmo.clusterCenters(1))
//    val arr3 = DenseVector(Array[Double](2,2))
//    val arr4 = DenseVector(Array[Double](-3,-3))
//    val arr5 = Array[DenseVector](arr3,arr4)
//    val newmodel  = skmeans.setInitialCenters(arr5,Array(8,5))
//    newmodel.predictOn(socketText).print()

//    val model = skmeans.latestModel()
//    System.out.println(model.clusterCenters(1))

//    skm.train(text)
//    skmeans.predictOn(socketText).print()

//    val center = model.clusterCenters
//    val weight = model.clusterWeights
//    System.out.print("~~~~"+weight(1))
//    System.out.print("~~~~"+center(1))

  }
//  case class VectorTest(var list: Array[String]) extends Serializable{
//      var list: Array[Double]
//  }


}
