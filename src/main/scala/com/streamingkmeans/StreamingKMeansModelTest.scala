package com.streamingkmeans

import java.util.StringTokenizer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamingKMeansModelTest {
  def main(args: Array[String]): Unit = {

    val modelinput = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/model.txt"


    // get the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val model: DataSet[String] = env.readTextFile(modelinput)
//    val a = model.map(s=>s.split("DenseVector").toList)
    model.print()
//    env.execute()

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
//def parse(s: String): Any = {
//  val tokenizer = new StringTokenizer(s, "()[],", true)
//  if (tokenizer.hasMoreTokens()) {
//    val token = tokenizer.nextToken()
//    if (token == "(") {
//      parseTuple(tokenizer)
//    } else if (token == "[") {
//      parseArray(tokenizer)
//    } else {
//      // expecting a number
//      parseDouble(token)
//    }
//  } else {
//    throw new SparkException(s"Cannot find any token from the input string.")
//  }
//}
}
