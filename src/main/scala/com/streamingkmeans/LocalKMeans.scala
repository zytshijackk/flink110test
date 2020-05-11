package ml.kmeans

import com.streamingkmeans.utils.EuclideanDistanceMeasure
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.ml.math.BLAS

import scala.util.Random

object LocalKMeans extends Serializable {
  def kMeansPlusPlus(
                      seed: Long,
                      points: Array[SparseVector],
                      weights: Array[Double],
                      k: Int,
                      maxIterations: Int
                    ): Array[SparseVector] = {
    val rand = new Random(seed)
    val dimensions = points(0).size
    val centers = new Array[SparseVector](k)

    // Initialize centers by sampling using the k-means++ procedure.
    centers(0) = pickWeighted(rand, points, weights)
    //D(x):各点到最近簇中心（第一个簇）的距离的集合
    val costArray: Array[Double] = points.map(EuclideanDistanceMeasure.fastSquaredDistance(_, centers(0)))
    System.out.println("center0"+centers(0))
    for (i <- 1 until k) {//取k个簇中心
      val sum: Double = costArray.zip(weights).map(p => p._1 * p._2).sum//Sum(D(x)):把点*权重进行求和
      val r = rand.nextDouble() * sum
      var currSum = 0.0
      var j = 0
      while (j < points.length && currSum < r) {
        currSum += weights(j) * costArray(j)
        j += 1
      }//当currSum > r 时结束，取出
      if (j == 0) {
        centers(i) = points(0)
      } else {
        centers(i) = points(j - 1)
      }

      // 现在多了一个下标为i的簇中心，所以最近簇距离要更新
      for (p <- points.indices) {
        costArray(p) = java.lang.Math.min(EuclideanDistanceMeasure.fastSquaredDistance(points(p), centers(i)), costArray(p))
      }
      System.out.println("center"+i+centers(i))
    }

    // Run up to maxIterations iterations of Lloyd's algorithm
//    val oldClosest = Array.fill(points.length)(-1)
//    var iteration = 0
//    var moved = true
//    while (moved && iteration < maxIterations) {
//      moved = false
//      val counts = Array.fill(k)(0.0)
//      //sums(index)存的是与簇中心index最近的点的权重*点之和
//      val sums: Array[DenseVector] = Array.fill(k)(DenseVector.zeros(dimensions))
//      var i = 0
//      while (i < points.length) {//遍历所有的点
//        val p = points(i)
//        val index = EuclideanDistanceMeasure.findClosest1(p, centers)._1
//        BLAS.axpy(weights(i), p, sums(index))//sums(index) +=  weights(i) * p(i)
//        counts(index) += weights(i) //存的是下标index簇中心的权重和
//        if (index != oldClosest(i)) {
//          moved = true
//          oldClosest(i) = index //oldClosest(i)存的是下标i的点的最近簇中心的下标
//        }
//        i += 1
//      }
//      // Update centers
//      var j = 0
//      while (j < k) {
//        if (counts(j) == 0.0) {
//          // Assign center to a random point
//          centers(j) = points(rand.nextInt(points.length))
//        } else {
//          BLAS.scal(1.0 / counts(j), sums(j))//sums(j) = 1.0 * sums(j) / counts(j)
//          centers(j) = sums(j).toSparseVector
//        }
//        j += 1
//      }
//      iteration += 1
//    }
    centers
  }

//  def costAverage(points: Array[SparseVector],
//                  centers: Array[SparseVector],
//                  weights: Array[Double]): Double = {
//    val cost = points
//      .map(EuclideanDistanceMeasure.findClosest1(_, centers))
//      .map(_._2)
//      .zip(weights)
//      .map(p => p._1 * p._2)
//      .sum / weights.sum
//    cost
//  }

  /**
   * 取一个随机值，用权重的方式来取计算下一个"种子点"。
   * 先用Sum(D(X))乘以随机值Random得到r,然后用currSum += D(X)，
   * 直到其currSum > r ，此时的点就是下一个"种子点"
   * @param rand
   * @param data
   * @param weights
   * @tparam T
   * @return
   */
  private def pickWeighted[T](rand: Random, data: Array[T], weights: Array[Double]): T = {
    val r = rand.nextDouble() * weights.sum
    System.out.println("r:"+r)
    var i = 0
    var curWeight = 0.0
    while (i < data.length && curWeight < r) {
      curWeight += weights(i)
      i += 1
    }
    System.out.println("i:"+i)
    data(i - 1)
  }
}
