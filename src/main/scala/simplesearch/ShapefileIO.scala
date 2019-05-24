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
import org.geotools.data.shapefile.ShapefileDataStore

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource, SimpleFeatureStore}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
//import org.opengis.feature.simple._
//import org.geotools.feature.simple._
//import org.geotools.feature._
//import org.opengis.feature.`type`.Name
import java.util.HashMap

import geotrellis.spark.{Metadata, SpatialKey, TileLayerMetadata}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

//import simpletiler.Constants._
//import simpletiler.UtilFunctions._


object ShapefileIO {
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

  def readSimpleFeatures(path: String) = {
    """
      | Copied for reference from geotrellis.shapefile
    """.stripMargin
    // Extract the features as GeoTools 'SimpleFeatures'
    val url = s"file://${new File(path).getAbsolutePath}"
    val ds = new ShapefileDataStore(new URL(url))
    val ftItr: SimpleFeatureIterator = ds.getFeatureSource.getFeatures.features

    try {
      val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
      while(ftItr.hasNext) simpleFeatures += ftItr.next()
      simpleFeatures.toList
    } finally {
      ftItr.close
      ds.dispose
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


  def readShapefileFromHDFS(shp_path: String)
//                               (implicit sc: SparkContext): RDD[MultiPolygonFeature[Map[String, Object]]] = {
                           (implicit sc: SparkContext): Unit = {
    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)
  }
}
