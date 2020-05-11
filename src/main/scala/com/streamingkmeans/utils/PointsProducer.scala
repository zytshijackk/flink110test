package com.streamingkmeans.utils

import java.io.{File, PrintWriter}

import org.apache.flink.util.XORShiftRandom

/**
 * @Author: ch
 * @Date: 06/05/2020 8:07 PM
 * @Version 1.0
 */
object PointProducer {
  def main(args: Array[String]): Unit = {
    val path = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/randompoint.txt"
    producePoint(path,3,608000)//608000=10MB
  }
  //dim生成数据的维度,pointSize产生多少个点
  def producePoint(path:String,dim:Int,pointsSize:Int): Unit ={
    val file = new File(path)
    val writer = new PrintWriter(file)
    val time =System.currentTimeMillis()
    val random = new XORShiftRandom(time)
    for(j <- 1 to pointsSize){
      var gaussian = ""
      for(i <- 1 to dim){
        gaussian += random.nextGaussian().formatted("%.2f").toString //保留两位
        if(i!=dim)
          gaussian+=" "
        else if(j != pointsSize)
          gaussian+="\n"
      }
      writer.write(gaussian)
    }
    writer.close()
  }
}
