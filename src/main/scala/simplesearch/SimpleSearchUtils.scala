package simplesearch

import geotrellis.spark.io.hadoop._
import org.apache.spark._
import org.apache.hadoop.fs.Path
import java.net.URI

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{LongAccumulator, SizeEstimator}


object SimpleSearchUtils {
  def hdfsListFiles(sc: SparkContext, uris: Array[URI], extensions: Seq[String]): Array[String] =
    uris.flatMap { uri => hdfsListFiles(sc, uri, extensions) }

  def hdfsListFiles(sc: SparkContext, uri: URI, extensions: Seq[String]): Array[String] = {
    val path: Path = new Path(uri)
    val conf = sc.hadoopConfiguration.withInputDirectory(path, extensions)

    HdfsUtils
      .listFiles(path, conf)
      .map { _.toString }
      .filter { path => extensions.exists { e => path.endsWith(e) } }
      .toArray
  }
  def measureMapSegNanoTime[T, R](action: T => R, acc: LongAccumulator): T => R = input => {
    val t1 = System.nanoTime
    val result = action(input)
    val t2 = System.nanoTime
    acc.add(t2 - t1)
    SingleLogging.log_metric("ACC",acc.value.toString)
    result
  }

  def measureNanoTime[R](f: => R): (R, Long) = {
    val t1 = System.nanoTime
    val ret = f
    val t2 = System.nanoTime
    (ret, t2 - t1)
  }

  def getRDDSize[V](rdd: RDD[V]) : Long = {
    var rddSize = 0l
    val rows = rdd.collect()
    for (i <- 0 until rows.length) {
      rddSize += SizeEstimator.estimate(rows.apply(i).asInstanceOf)
    }

    rddSize
  }

}