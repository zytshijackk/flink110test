package com.streamingkmeans

import com.streamingkmeans.utils.{StringToDense, StringToSparse}
import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
object StreamingKMeansTest {
  def main(args: Array[String]): Unit = {

    val input = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/point.txt"

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text:DataStream[String] = env.readTextFile(input)
//    val list:DataStream[Array[Double]]  = text
//      .flatMap(new FlatMapFunction[String,Array[Double]] {
//        override def flatMap(t: String, collector: Collector[Array[Double]]): Unit = {
//          val  word:Array[Double] = t.split("\\s").map(_.toDouble)
//          collector.collect(word)
//        }
//      })

//    val dense:DataStream[DenseVector] = text.map(new StringToDense)

//      dense.countWindowAll(5)
//      .apply{( window: GlobalWindow,
//               events: SparseVector,
//               out: Collector[(Double, Long)]) =>
//        out.collect(update(events)))
//      }

//    dense.print()

//    re.print()
//    list.map(a=>)
//    text.print()
//    val win:AllWindowedStream[Array[String],TimeWindow]  = list.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//    val win:AllWindowedStream[Array[String],GlobalWindow] = list.countWindowAll(5)
//    val arr = Array(List(1,1),List(-1,-1),List(0,0))
    val arr1 = DenseVector(Array[Double](1,1))
    val arr2 = DenseVector(Array[Double](-1,-1))
    val arr = Array[DenseVector](arr1,arr2)
    val skm = new StreamingKMeans(2,2,1,arr,Array(3.0,3.0))

    skm.trainOnStream(text)

    env.execute()
  }
//  case class VectorTest(var list: Array[String]) extends Serializable{
//      var list: Array[Double]
//  }
}
