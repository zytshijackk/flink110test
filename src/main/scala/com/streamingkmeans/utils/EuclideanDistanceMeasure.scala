package com.streamingkmeans.utils

import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector}

//欧氏距离
object EuclideanDistanceMeasure{

  def distance(point: DenseVector, center: DenseVector): Double = {
    //    for(x<-point){
    //      System.out.println(x)
    //    }
    var a = 0.0
    for (i <- 0 to center.size - 1) {

      a += (center(i) - point(i)) * (center(i) - point(i)) //差的平方

    }

    math.sqrt(a)
  }
  def distance2(point: DenseVector, center: DenseVector): Double = {
    var a = 0.0
    for (i <- 0 to center.size - 1) {
      a += (center(i) - point(i)) * (center(i) - point(i)) //差的平方
    }
    a
  }
  //求两个向量的欧式距离
  def fastSquaredDistance(v1: SparseVector, v2: SparseVector): Double = {
    val n = v1.size
    require(v2.size == n)
    val norm1 = new VectorWith2Norm(v1).norm2()
    val norm2 = new VectorWith2Norm(v2).norm2()
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2 // a..^2 + b..^2
    val normDiff = norm1 - norm2 // √(a..^2) - √(b..^2)
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val EPSILON = {
      var eps = 1.0
      while ((1.0 + (eps / 2.0)) != 1.0) {
        eps /= 2.0
      }
      eps
    }
    val precision = 1e-6

    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {//为了防止精度丢失的判断
      sqDist = sumSquaredNorm - 2.0 * v1.dot(v2) //a^2 + b^2-2*a*b.
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = v1.dot(v2)
      sqDist = java.lang.Math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * java.lang.Math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = sqdist(v1, v2)
      }
    } else {
      sqDist = sqdist(v1, v2)
    }
    sqDist
  }
  private def sqdist(v1: SparseVector, v2: SparseVector): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0

    val v1Values = v1.data
    val v1Indices = v1.indices
    val v2Values = v2.data
    val v2Indices = v2.indices
    val nnzv1 = v1Indices.length
    val nnzv2 = v2Indices.length

    var kv1 = 0
    var kv2 = 0
    while (kv1 < nnzv1 || kv2 < nnzv2) {
      var score = 0.0

      if (kv2 >= nnzv2 || (kv1 < nnzv1 && v1Indices(kv1) < v2Indices(kv2))) {
        score = v1Values(kv1)
        kv1 += 1
      } else if (kv1 >= nnzv1 || (kv2 < nnzv2 && v2Indices(kv2) < v1Indices(kv1))) {
        score = v2Values(kv2)
        kv2 += 1
      } else {
        score = v1Values(kv1) - v2Values(kv2)
        kv1 += 1
        kv2 += 1
      }
      squaredDistance += score * score
    }

    squaredDistance
  }

  /**
   * 返回最近的Center对应的ID和距离
   *
   * @param centers
   * @param point
   * @return
   */
  def findClosest(
                    point: DenseVector,
                    centers: Array[DenseVector]
                  ): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
//      val currentDistance = distance(point, center)
      val currentDistance = distance2(point, center)
      if (currentDistance < bestDistance) {
        bestDistance = currentDistance
        bestIndex = i
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  def findClosest1(point: SparseVector, centers: Array[SparseVector]): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    for (center: SparseVector <- centers) {
      var lowerBoundOfSqDist = new VectorWith2Norm(center).norm2 - new VectorWith2Norm(point).norm2()
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }
  def pointCost(
                 centers: Array[DenseVector],
                 point: DenseVector): Double = {
    findClosest( point,centers)._2
  }

  def addDenseVector(_2: DenseVector, _21: DenseVector):DenseVector = {
    BLAS.axpy(1,_2,_21) //y += a*x
    _21
  }
  def divDenseVector(_2: DenseVector, _21: Long):DenseVector = {
    System.out.println(_2)
    System.out.println(_21)
    val a = BLAS.scal( 1/_21 , _2 ) // x= a*x
//    System.out.println(a)
    _2
  }
}


//求了稀疏向量的  √( x1^2 + x2^2 + ... + xn^2 ) 也就是二范式
class VectorWith2Norm(val vector: SparseVector) extends Serializable {
  def norm2(): Double = {
    val value = vector.data
    val indices = vector.indices
    var i: Int = 0
    var norm: Double = 0.0
    while (i < vector.indices.length) {
      norm += value(i) * value(i)
      i += 1
    }
    java.lang.Math.sqrt(norm)
  }
}
