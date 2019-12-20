package simplesearch

import simplesearch.SimpleSearchUtils
import geotrellis.spark.io.hadoop._
import org.geotools.data.simple
import org.geotools.data.shapefile._
import org.opengis.feature.simple._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.fs.Path
import java.io.File
import java.net.{URI, URL}

import org.apache.hadoop.fs

import scala.collection.mutable


object HadoopShapefileRDD {
  def createSimpleFeaturesRDD(
                               sc: SparkContext,
                               uris: Array[URI],
                               extensions: Seq[String],
                               numPartitions: Int
                             ): RDD[SimpleFeature] =
    createSimpleFeaturesRDD(sc, SimpleSearchUtils.listFiles(sc, uris, extensions), numPartitions)

  def createSimpleFeaturesRDD(
                               sc: SparkContext,
                               paths: Array[String],
                               numPartitions: Int
                             ): RDD[SimpleFeature] = {
    val urls = sc.parallelize(paths, numPartitions).map { new URL(_) }
    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)

    urls.flatMap { url =>
      val ds = new ShapefileDataStore(url)
      val ftItr = ds.getFeatureSource.getFeatures.features

      try {
        val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
        while(ftItr.hasNext) simpleFeatures += ftItr.next()
        simpleFeatures.toList
      } finally {
        ftItr.close
        ds.dispose
      }
    }
  }
}
