package simplesearch

import geotrellis.spark.{MultibandTileLayerRDD, SpatialKey}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator

import scala.io.StdIn
import simplesearch.ShapefileIO._
import simplesearch.RasterIO._
import simplesearch.SimpleTileIndexQuery._

import scala.compat.java8.collectionImpl.LongAccumulator


object ContextKeeper  {
  val conf = new SparkConf()
    .setAppName("SimpleSearch")
    //.setMaster("local[2]")
    .setMaster("spark://spark00:7077")
    .set("spark.submit.deployMode", "client")
    //.set("spark.submit.deployMode", "cluster")
    .set("spark.sql.defaultUrlStreamHandlerFactory.enabled","true")
    .set("spark.serializer",        classOf[KryoSerializer].getName)
    .set("spark.kryo.registrator",  classOf[KryoRegistrator].getName)
    //.set("spark.kryo.registrator",  classOf[CustomRegistrator].getName)
    .set("spark.yarn.am.memory", "1024m")
    .set("spark.driver.memory", "4096m")
    .set("spark.executor.memory", "3120m")
    .set("spark.executor.cores", "2")
//    .set("spark.cores.max", "2")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/ubuntu/spark-logs")
    .set("spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider")
    .set("spark.history.fs.logDirectory", "/home/ubuntu/spark-logs")
    .set("spark.history.fs.update.interval", "3s")
    .set("spark.history.ui.port", "18080")
    .set("spark.ui.enabled", "true")
    .set("spark.driver.port","20002")
    .set("spark.driver.host","spark00")

    /** TODO: Learn to add to KryoRegistrator all serializable classes declared
      *  e.g. http://web.cs.ucla.edu/~harryxu/papers/nguyen-asplos18.pdf
      *  use/build from geotrellis.spark.io.kryo.KryoRegistrator
      *  TODO: or export JAR file to HDFS and use spark-submit to execute, it may be because
      **///
    //.set("spark.default.parallelism", "2")
    //.set("spark.akka.frameSize", "512")
    .set("spark.kryoserializer.buffer.max", "1024m")
  val context= new SparkContext(conf)

}


// Force the globals object to be initialized
import ContextKeeper._

object Main {



  def createIntArray(len: Int): Array[Int] = {
    return Array[Int](len)
  }

  def main(args: Array[String]): Unit = {
    //Initialize
    println("\n\n>>> INITIALIZING <<<\n\n")

    // Initializes context only for Spark Driver
//    implicit val sc = new SparkContext(createAllSparkConf())
//    implicit val sc = new SparkContext()

    //
//    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)
    println("ARGUMENTS:")
    pprint.pprintln(args)

    val wait_on_finish = args(0).toBoolean
    val num_executors = args(1).toInt
    val run_reps = args(2).toInt

    //implicit val sc : SparkContext = ContextKeeper.context
    var sparkconf :SparkConf = ContextKeeper.context.getConf

    println("Proper registrator names: \n[" + classOf[KryoSerializer].getName +"]\n["+classOf[KryoRegistrator].getName+"]")
    println("Spark Config: \n" + sparkconf.toDebugString)

    // Dummy RDD used for initializing SparkContext in executors
    //var init_rdd : RDD[Int] = sc.parallelize(createIntArray(num_executors))\
    /**
    var init_rdd : RDD[Int] = ContextKeeper.context.parallelize(createIntArray(num_executors))
    init_rdd.foreachPartition { partition =>
      //implicit val sc = ContextKeeper.context
      ContextKeeper.context
    }
    */
    val time_acc = ContextKeeper.context.longAccumulator("Timer Accumulator")

    try {




      args(3) match {
//        case "read" => readShapefileFromFilepath(
//          args(2))
        case "read_shp" => readMultiPolygonFeatures(
          args(4)) (ContextKeeper.context)

        case "test_shp" => writeShapefileIntoFilepath(
          args(4), args(5), args(6))(ContextKeeper.context)
        //        case "find" => run_read_find_feature(
        //          run_reps, args(3),args(4,args(5),args(6),args(7))

        case "read_gtiff" => readGeotiffFromFilepath(
          args(4))(ContextKeeper.context, time_acc)

        case "query_gtiff_w_shp" => {
          val (qry_ft, nanotime_1) = SimpleSearchUtils.measureNanoTime(createMultiPolyFeatures(
            args(4), Constants.RDD_PARTS)(0))

          SingleLogging.log_metric("CREATE_RDD_NANOTIME_MPFEATURES", nanotime_1.toString)
          SingleLogging.log_metric("SIZEESTIMATE_MPFEATURES", SizeEstimator.estimate(qry_ft).toString)
          
          val (input_gtiff, nanotime_2) = SimpleSearchUtils.measureNanoTime(readGeotiffFromFilepath(
            args(5))(ContextKeeper.context, time_acc))

          SingleLogging.log_metric("CREATE_RDD_NANOTIME_GEOTIFF", nanotime_2.toString)
          SingleLogging.log_metric("SIZEESTIMATE_GEOTIFF", SizeEstimator.estimate(input_gtiff).toString)
          // Temp debug count to input geotiff rdd
          SingleLogging.log_metric("SIZEOF_INPUT_RDD", SimpleSearchUtils.getRDDSize(input_gtiff.values).toString)
          SingleLogging.log_metric("COUNT_INPUT_RDD", input_gtiff.count.toString)


          val (result_gtiff_rdd : MultibandTileLayerRDD[SpatialKey], nanotime_3) = SimpleSearchUtils.measureNanoTime(queryGeoTiffWithShp(qry_ft, input_gtiff)(ContextKeeper.context, time_acc))

          SingleLogging.log_metric("CREATE_RDD_NANOTIME_RESULT_GEOTIFF", nanotime_3.toString)
          SingleLogging.log_metric("SIZEESTIMATE_RESULT_GEOTIFF",SizeEstimator.estimate(result_gtiff_rdd).toString)

          // Prints out Spatial Keys
          SingleLogging.log_metric("SIZEOF_RESULT_RDD", SimpleSearchUtils.getRDDSize(result_gtiff_rdd.values).toString)
          SingleLogging.log_metric("COUNT_RESULT_RDD", result_gtiff_rdd.count.toString)
          result_gtiff_rdd.foreach{ mbtl =>
            val spatial_key = mbtl._1
            println("Spatial Key: [" + spatial_key.row.toString + "," + spatial_key.col.toString+"]")
          }
        }

        case "query_gtiff_w_shp_rdd" => {
          val (qry_ft_rdd, nanotime_1) = SimpleSearchUtils.measureNanoTime(readMultiPolygonFeatures(
            args(4))(ContextKeeper.context))

          SingleLogging.log_metric("CREATE_RDD_NANOTIME_MPFEATURES", nanotime_1.toString)
          SingleLogging.log_metric("SIZEESTIMATE_MPFEATURES", SizeEstimator.estimate(qry_ft_rdd).toString)

          val (input_gtiff, nanotime_2) = SimpleSearchUtils.measureNanoTime(readGeotiffFromFilepath(
            args(5))(ContextKeeper.context, time_acc))

          SingleLogging.log_metric("CREATE_RDD_NANOTIME_GEOTIFF", nanotime_2.toString)
          SingleLogging.log_metric("SIZEESTIMATE_GEOTIFF", SizeEstimator.estimate(input_gtiff).toString)
          // Temp debug count to input geotiff rdd
          SingleLogging.log_metric("COUNT_RESULT_RDD", input_gtiff.count.toString)

          val (result_gtiff_rdd, nanotime_3) = SimpleSearchUtils.measureNanoTime(queryGeoTiffWithShpRDD(qry_ft_rdd, input_gtiff)
            (ContextKeeper.context, time_acc))

          SingleLogging.log_metric("CREATE_RDD_NANOTIME_RESULT_GEOTIFF", nanotime_3.toString)
          SingleLogging.log_metric("SIZEESTIMATE_RESULT_GEOTIFF",SizeEstimator.estimate(result_gtiff_rdd).toString)

          // Prints out Spatial Keys
          SingleLogging.log_metric("COUNT_RESULT_RDD", result_gtiff_rdd.count.toString)
          result_gtiff_rdd.foreach{ mbtl =>
            val spatial_key = mbtl._1
            println("Spatial Key: [" + spatial_key.row.toString + "," + spatial_key.col.toString+"]")
          }
        }


        case _ => println("ERROR: Invalid CLI arg(2)")
      }
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040

      // Delete temp directory if it exists
      //if (tmp_dir.exists) tmp_dir.delete()
      if(wait_on_finish) {
        println("Hit enter to exit.")
        StdIn.readLine()
      }
    } finally {
      //sc.stop()
      (ContextKeeper.context)
    }
  }
}
