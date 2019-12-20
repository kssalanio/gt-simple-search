package simplesearch

import java.io.{BufferedOutputStream, File, PrintWriter}
import java.net.URL

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.GeometryFactory
import geotrellis.raster.MultibandTile
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark.io.hadoop._
import geotrellis.vector.{MultiPolygon, MultiPolygonFeature, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.geotools.data.{DataStoreFinder, FeatureSource, FeatureWriter, Transaction}
import org.apache.hadoop.fs

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource, SimpleFeatureStore}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.simple._
import org.opengis.feature.simple._
import org.geotools.data.shapefile._
import com.vividsolutions.jts.{geom => jts}

import java.util.HashMap

import geotrellis.spark.{Metadata, SpatialKey, TileLayerMetadata}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import org.apache.hadoop.fs._
import org.apache.commons.io.IOUtils

//import simpletiler.Constants._
//import simpletiler.UtilFunctions._

import geotrellis.vector._

import org.geotools.data.simple._
import org.opengis.feature.simple._
import org.geotools.data.shapefile._
import com.vividsolutions.jts.{geom => jts}

import java.net.{URI, URL}
import java.io.File

import scala.collection.mutable
import scala.collection.JavaConversions._

import scala.collection.mutable
import simplesearch.HadoopShapefileRDD._


object ShapefileIO {
  implicit class SimpleFeatureWrapper(ft: SimpleFeature) {
    def geom[G <: jts.Geometry: Manifest]: Option[G] =
      ft.getAttribute(0) match {
        case g: G => Some(g)
        case _ => None
      }

    def attributeMap: Map[String, Object] =
      ft.getProperties.drop(1).map { p =>
        (p.getName.toString, ft.getAttribute(p.getName))
      }.toMap

    def attribute[D](name: String): D =
      ft.getAttribute(name).asInstanceOf[D]
  }

  def readShapefileFromFilepath(shp_path: String)
                  (implicit sc: SparkContext): RDD[MultiPolygonFeature[Map[String, Object]]] = {

    val features: Seq[MultiPolygonFeature[Map[String, Object]]] = ShapeFileReader.readMultiPolygonFeatures(shp_path)
    println("METRIC: sizeEstimate - features: "+SizeEstimator.estimate(features).toString)
    features.foreach{ ft =>
      val region: MultiPolygon = ft.geom
      val query_extents = ft.envelope
      val attribute_table = ft.data
      val block_name = attribute_table("BLOCK_NAME")
      println(s"VECTOR: Feature [${block_name}] has [${region.vertexCount}] points.")
    }

    return sc.parallelize(features)
  }

  def readMultiPolygonFeatures(path: String)
                              (implicit sc: SparkContext): RDD[MultiPolygonFeature[Map[String, Object]]] = {
    """
      | Copied for reference from geotrellis.shapefile
    """.stripMargin
    // Extract the features as GeoTools 'SimpleFeatures'
    var url = ""
    var mp_features = None
    // Reads from HDFS
    if(path contains "hdfs"){
      //var paths = Array(path)
      var numPartitions = 10;
      var mp_features :RDD[MultiPolygonFeature[Map[String, Object]]] = createMultiPolyFeatures(
        path: String,
        numPartitions: Int) (sc: SparkContext)
      println("METRIC: sizeEstimate - features: "+SizeEstimator.estimate(mp_features).toString)
      mp_features.foreach{ ft=>
        println(">>> "+ ft.data("NAME_2").toString + " -- " +SizeEstimator.estimate(ft).toString)
      }
      mp_features
    }else { // Reads from File Path
      url = s"file://${new File(path).getAbsolutePath}"

      val ds = new ShapefileDataStore(new URL(url))
      val ftItr: SimpleFeatureIterator = ds.getFeatureSource.getFeatures.features

      try {
        val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
        while (ftItr.hasNext) simpleFeatures += ftItr.next()
        simpleFeatures.toList
        println("METRIC: sizeEstimate - features: " + SizeEstimator.estimate(simpleFeatures).toString)
        var mp_features: Seq[MultiPolygonFeature[Map[String, Object]]] = simpleFeatures.flatMap { ft => ft.geom[jts.MultiPolygon].map(MultiPolygonFeature(_, ft.attributeMap)) }
        sc.parallelize(mp_features)
      } finally {
        ftItr.close
        ds.dispose
      }
    }
  }

  def writeShapefileIntoFilepath(src_shp_path: String, dst_shp_path: String, schema_shp_path: String)
                                (implicit sc: SparkContext): Unit = {

    val features_seq: Seq[MultiPolygonFeature[Map[String, Object]]] = ShapeFileReader.readMultiPolygonFeatures(src_shp_path)

    val in_url = s"file://${new File(schema_shp_path).getAbsolutePath}"
    val in_ds = new ShapefileDataStore(new URL(in_url))
    val input_type_name: String  = in_ds.getTypeNames()(0)
    val input_type: SimpleFeatureType  = in_ds.getSchema(input_type_name)

    println(s"SCHEMA: ${input_type_name}")

    // Output Shapefile
    val out_url = s"file://${new File(dst_shp_path).getAbsolutePath}"
    val out_ds = new ShapefileDataStore(new URL(out_url))
    out_ds.createSchema(input_type)
    val type_name: String = out_ds.getTypeNames()(0)

    val out_fs: SimpleFeatureSource = out_ds.getFeatureSource()
    val out_fc: SimpleFeatureCollection = out_fs.getFeatures()

    println("OUT SCHEMA DETAILS:")
    pprint.pprintln(out_ds.getSchema.toString)

    val out_ft_writer: FeatureWriter[SimpleFeatureType, SimpleFeature] = out_ds.getFeatureWriter(out_ds.getTypeNames()(0), Transaction.AUTO_COMMIT);


    val geom_factory = new GeometryFactory()

    //TODO: Code for creating schema

    // For each feature
    println("Writing features:")
    features_seq.foreach{
      ft =>
        val mp_feat = new MultiPolygonFeature(ft.geom.jtsGeom, ft.data)
        val write_feat: SimpleFeature = out_ft_writer.next // get next empty feature to write to

        val geom_feat: geom.MultiPolygon = ft.geom.jtsGeom // set the geometry, but first convert to JTS geometry
        println("\n* Geometry: "+geom_feat.getLength())
        write_feat.setDefaultGeometry(geom_feat)

        // write the attribute table
        ft.data.foreach{
          case (attr_key: String, attr_val: Object) =>
            println("> "+attr_key.toString()+" || "+attr_val.toString())
            write_feat.setAttribute(attr_key, attr_val)
        }
    }

    out_ft_writer.write() // Write the shapefile to file

  }



  def createMultiPolyFeatures(
                               path: String,
                               numPartitions: Int
                             )(implicit  sc: SparkContext): RDD[MultiPolygonFeature[Map[String, Object]]] = {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)
    val url = new URL(path)
    val ds = new ShapefileDataStore(url)
    val ftItr = ds.getFeatureSource.getFeatures.features

    try {
      val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
      while(ftItr.hasNext) simpleFeatures += ftItr.next()
      val mp_features : Seq[MultiPolygonFeature[Map[String, Object]]] = simpleFeatures.flatMap { ft => ft.geom[jts.MultiPolygon].map(MultiPolygonFeature(_, ft.attributeMap)) }
      sc.parallelize(mp_features)
    } finally {
      ftItr.close
      ds.dispose
    }
  }
}
