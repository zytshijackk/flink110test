package com.streamingkmeans

import com.clustering.utils.KMeansData
import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * K-Means是一种迭代聚类算法，其工作原理如下:
 * K-means给出一组待聚类的数据点和一组初始的K簇中心。
 * 在每次迭代中，算法计算每个数据点到每个簇中心的距离。
 * 每个点都被分配到离它最近的聚类中心。
 * 随后，将每个聚类中心移动到分配给它的所有点的中心(均值)。
 * 移动的聚类中心被输入到下一次迭代中。
 * 算法在固定次数的迭代后终止(在本实现中)
 * 或者如果聚类中心在一次迭代中没有(显著)移动。
 * 这是<a href="http://en.wikipedia.org/wiki/K-means_clustering"的Wikipedia条目，>K-Means集群算法</a>。
 *
 * 这个实现工作在二维数据点上。
 * 计算向聚类中心分配的数据点，即
 * 每个数据点都使用其所属的最终聚类(中心)的id进行注释。
 *
 * 输入文件为纯文本文件，格式必须如下:
 *
 * 数据点表示为两个由空白字符分隔的双值。
 * 数据点由换行符分隔。
 * 例如"1.2 2.3\n5.3 7.2\n"给出两个数据点(x=1.2, y=2.3)和(x=5.3, y=7.2)。
 * 聚类中心由一个整数id和一个点值表示。
 * *例如"1 6.2 3.2\n2 2.9 5.7\n"给出了两个中心(id=1, x=6.2, y=3.2)和(id=2, x=2.9, y=5.7)。
 *
 * 用法: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * 如果没有提供参数，则使用来自{@link org.apache.flink.examples.java.clustering.util.KMeansData}和10次迭代。
 *
 * This example shows how to use:
 *
 * - Bulk iterations
 * - Broadcast variables in bulk iterations
 * - Scala case classes
 */
object KMeans {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // get input data:
    // read the points and centroids from the provided paths or fall back to default data
    val points: DataSet[Point] = getPointDataSet(params, env)
    val centroids: DataSet[Centroid] = getCentroidDataSet(params, env)

    val finalCentroids = centroids.iterate(params.getInt("iterations", 10)) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }
      .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2.add(p2._2), p1._3 + p2._3) }
        .map { x => new Centroid(x._1, x._2.div(x._3)) }
      newCentroids
    }

    val clusteredPoints: DataSet[(Int, Point)] =
      points.map(new SelectNearestCenter).withBroadcastSet(finalCentroids, "centroids")

    if (params.has("output")) {
      clusteredPoints.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Scala KMeans Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      clusteredPoints.print()
    }

  }

  def getCentroidDataSet(params: ParameterTool, env: ExecutionEnvironment): DataSet[Centroid] = {
    if (params.has("centroids")) {
      env.readCsvFile[Centroid](
        params.get("centroids"),
        fieldDelimiter = " ",
        includedFields = Array(0, 1, 2))
    } else {
      println("Executing K-Means example with default centroid data set.")
      println("Use --centroids to specify file input.")
      env.fromCollection(KMeansData.CENTROIDS map {
        case Array(id, x, y) =>
          new Centroid(id.asInstanceOf[Int], x.asInstanceOf[Double], y.asInstanceOf[Double])
      })
    }
  }

  def getPointDataSet(params: ParameterTool, env: ExecutionEnvironment): DataSet[Point] = {
    if (params.has("points")) {
      env.readCsvFile[Point](
        params.get("points"),
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
    } else {
      println("Executing K-Means example with default points data set.")
      println("Use --points to specify file input.")
      env.fromCollection(KMeansData.POINTS map {
        case Array(x, y) => new Point(x.asInstanceOf[Double], y.asInstanceOf[Double])
      })
    }
  }

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
   * Common trait for operations supported by both points and centroids
   * Note: case class inheritance is not allowed in Scala
   */
  trait Coordinate extends Serializable {

    var x: Double
    var y: Double

    def add(other: Coordinate): this.type = {
      x += other.x
      y += other.y
      this
    }

    def div(other: Long): this.type = {
      x /= other
      y /= other
      this
    }

    def euclideanDistance(other: Coordinate): Double =
      Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y))

    def clear(): Unit = {
      x = 0
      y = 0
    }

    override def toString: String =
      s"$x $y"

  }

  /**
   * A simple two-dimensional point.
   */
  case class Point(var x: Double = 0, var y: Double = 0) extends Coordinate

  /**
   * A simple two-dimensional centroid, basically a point with an ID.
   */
  case class Centroid(var id: Int = 0, var x: Double = 0, var y: Double = 0) extends Coordinate {

    def this(id: Int, p: Point) {
      this(id, p.x, p.y)
    }

    override def toString: String =
      s"$id ${super.toString}"

  }

  /** Determines the closest cluster center for a data point. */
  @ForwardedFields(Array("*->_2"))
  final class SelectNearestCenter extends RichMapFunction[Point, (Int, Point)] {
    private var centroids: Traversable[Centroid] = null
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }
    def map(p: Point): (Int, Point) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for (centroid <- centroids) {
        val distance = p.euclideanDistance(centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = centroid.id
        }
      }
      (closestCentroidId, p)
    }
  }
}