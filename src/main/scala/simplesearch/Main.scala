package simplesearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import geotrellis.spark.io.kryo.KryoRegistrator
import scala.io.StdIn
import simplesearch.ShapefileIO._



object Main {

  def createAllSparkConf(): SparkConf = {
    /**
      * # -- MEMORY ALLOCATION -- #
      *spark.master                   yarn
      *spark.driver.memory            512m
      *spark.yarn.am.memory           512m
      *spark.executor.memory          512m
      **
      *
      *# -- MONITORING -- #
      *spark.eventLog.enabled            true
      *spark.eventLog.dir                /home/ubuntu/spark-logs
      *spark.history.provider            org.apache.spark.deploy.history.FsHistoryProvider
      *spark.history.fs.logDirectory     /home/ubuntu/spark-logs
      *spark.history.fs.update.interval  3s
      *spark.history.ui.port             18080
      *spark.ui.enabled                  true
      *
      */
    new SparkConf()
      .setMaster("local[2]")
      //.setMaster("spark://spark00:7077")
      .set("spark.submit.deployMode", "client")
      //.setMaster("local")
      .setAppName("Thesis")
      .set("spark.serializer",        classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator",  classOf[KryoRegistrator].getName)
      .set("spark.yarn.am.memory", "1024m")
      .set("spark.driver.memory", "1024m")
      .set("spark.executor.memory", "1024m")
      .set("spark.executor.cores", "2")
      .set("spark.cores.max", "2")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/home/ubuntu/spark-logs")
      .set("spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider")
      .set("spark.history.fs.logDirectory", "/home/ubuntu/spark-logs")
      .set("spark.history.fs.update.interval", "3s")
      .set("spark.history.ui.port", "18080")
      .set("spark.ui.enabled", "true")
      .set("spark.driver.port","20002")
      .set("spark.driver.host","spark00")

    //.set("spark.default.parallelism", "2")
    //.set("spark.akka.frameSize", "512")
      .set("spark.kryoserializer.buffer.max", "1024m")

  }

  def main(args: Array[String]): Unit = {
    //Initialize
    println("\n\n>>> INITIALIZING <<<\n\n")

    implicit val sc = new SparkContext(createAllSparkConf())
    //    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)

    val run_reps = args(1).toInt

    try {

      println("ARGUMENTS:")
      pprint.pprintln(args)


      args(0) match {
//        case "read" => readShapefileFromFilepath(
//          args(2))
        case "read" => readSimpleFeatures(
          args(2))
        case "test_shp" => writeShapefileIntoFilepath(
          args(2), args(3), args(4))
        //        case "find" => run_read_find_feature(
        //          run_reps, args(2),args(3),args(4),args(5),args(6))

        case _ => println("ERROR: Invalid first CLI argument")
      }
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040

      // Delete temp directory if it exists
      //if (tmp_dir.exists) tmp_dir.delete()

      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }
}
