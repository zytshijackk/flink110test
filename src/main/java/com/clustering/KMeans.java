/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clustering;

import com.clustering.utils.KMeansData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Collection;

/**
 * This example implements a basic K-Means clustering algorithm.
 * K-Means分类算法的实现
 *
 * <p>K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
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
 * <p>This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 * 这个实现工作在二维数据点上。
 * 计算向聚类中心分配的数据点，即
 * 每个数据点都使用其所属的最终聚类(中心)的id进行注释。
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 * 输入文件为纯文本文件，格式必须如下:
 * 数据点表示为两个由空白字符分隔的双值。
 * 数据点由换行符分隔。
 * 例如"1.2 2.3\n5.3 7.2\n"给出两个数据点(x=1.2, y=2.3)和(x=5.3, y=7.2)。
 * 聚类中心由一个整数id和一个点值表示。
 * *例如"1 6.2 3.2\n2 2.9 5.7\n"给出了两个中心(id=1, x=6.2, y=3.2)和(id=2, x=2.9, y=5.7)。
 *
 * <p>Usage: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link com.clustering.utils.KMeansData} and 10 iterations.
 * 用法: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * 如果没有提供参数，则使用来自{@link com.clustering.utils.KMeansData}和10次迭代。
 *
 *
 * <p>This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 * 这个例子展示了如何使用:
 * 散装迭代
 * 广播变量在批量迭代
 * 自定义Java对象(pojo)
 */
@SuppressWarnings("serial")
public class KMeans {

	public static void main(String[] args) throws Exception {

		// Checking input parameterso
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		// get input data:
		// read the points and centroids from the provided paths or fall back to default data
		DataSet<Point> points = getPointDataSet(params, env);
		DataSet<Centroid> centroids = getCentroidDataSet(params, env);

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

		/**
		 * 迭代了10次求聚类中心
		 */
		DataSet<Centroid> newCentroids = points
			// compute closest centroid for each point. 返回Tuple2<Integer centridId,Point p>
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
			// count and sum point coordinates for each centroid. 返回Tuple3<Integer centridId,Point p,Long 1L>
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator()) //返回Tuple3<id,sum(p)，sum(count)>
			// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager());//返回Tuple2<id,sum(p)/sum(count)/>

		// feed new centroids back into next iteration
		// 将新的中心点反馈到下一次迭代中（就是结束迭代）
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		//为每个点寻找最近的聚类中心
		DataSet<Tuple2<Integer, Point>> clusteredPoints = points
			// assign points to final clusters
			.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		// emit result
		if (params.has("output")) {
			clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("KMeans Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			clusteredPoints.print();
		}
	}

	// *************************************************************************
	//     DATA SOURCE READING (POINTS AND CENTROIDS)
	//		数据源读取(点和中心点)
	// *************************************************************************

	private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
		DataSet<Centroid> centroids;
		if (params.has("centroids")) {
			centroids = env.readCsvFile(params.get("centroids"))
				.fieldDelimiter(" ")//配置分隔行中的字段分隔符。默认逗号
				.pojoType(Centroid.class, "id", "x", "y");//将读取器配置为读取CSV数据并将其解析为给定类型。类型的所有字段必须是公共的或能够设置值。字段的类型信息是从type类获得的。
		} else {
			System.out.println("Executing K-Means example with default centroid data set.");
			System.out.println("Use --centroids to specify file input.");
			centroids = KMeansData.getDefaultCentroidDataSet(env);
		}
		return centroids;
	}

	private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
		DataSet<Point> points;
		if (params.has("points")) {
			// read points from CSV file
			points = env.readCsvFile(params.get("points"))
				.fieldDelimiter(" ")
				.pojoType(Point.class, "x", "y");
		} else {
			System.out.println("Executing K-Means example with default point data set.");
			System.out.println("Use --points to specify file input.");
			points = KMeansData.getDefaultPointDataSet(env);
		}
		return points;
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * A simple two-dimensional point.
	 */
	public static class Point implements Serializable {

		public double x, y;

		public Point() {}

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public Point add(Point other) {
			x += other.x;
			y += other.y;
			return this;
		}

		public Point div(long val) {//除
			x /= val;
			y /= val;
			return this;
		}

		//欧式距离
		public double euclideanDistance(Point other) {
			return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
		}

		public void clear() {
			x = y = 0.0;
		}

		@Override
		public String toString() {
			return x + " " + y;
		}
	}

	/**
	 * A simple two-dimensional centroid, basically a point with an ID.
	 */
	public static class Centroid extends Point {

		public int id;

		public Centroid() {}

		public Centroid(int id, double x, double y) {
			super(x, y);
			this.id = id;
		}

		public Centroid(int id, Point p) {
			super(p.x, p.y);
			this.id = id;
		}

		@Override
		public String toString() {
			return id + " " + super.toString();
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/** Determines the closest cluster center for a data point. */
	/** 确定一个数据点最近的集群中心。 */
	@ForwardedFields("*->1")
	public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
		private Collection<Centroid> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		/** 将广播变量的质心值读入集合。*/
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Point> map(Point p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(centroid);

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.id;
				}
			}
			// emit a new record with the center id and the data point.
			return new Tuple2<>(closestCentroidId, p);
		}
	}

	/** Appends a count variable to the tuple. */
	/** 将计数变量附加到元组。*/
	@ForwardedFields("f0;f1")
	public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
			return new Tuple3<>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	@ForwardedFields("0")
	public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

		//point和count都累加
		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	@ForwardedFields("0->id")
	public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

		//sum(point)/count
		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}
}
