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

//    val input = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/point.txt"

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

    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream("127.0.0.1", port, '\n')
    val arr1 = DenseVector(Array[Double](1,1))
    val arr2 = DenseVector(Array[Double](-1,-1))
    val arr = Array[DenseVector](arr1,arr2)
    val skm = new StreamingKMeans2(2,2,0.6,arr,Array(3.0,2.0))
    skm.train(text)
    env.execute()
    val center = skm.clusterCenters
    val weight = skm.clusterWeights
    System.out.print("~~~~"+weight(1))
    System.out.print("~~~~"+center(1))

  }
//  case class VectorTest(var list: Array[String]) extends Serializable{
//      var list: Array[Double]
//  }
}
