package simplesearch

import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object ContextKeeper {
  val conf = new SparkConf()
    .setAppName("SimpleSearch")
    //.setMaster("local[2]")
    .setMaster("spark://spark00:7077")
    .set("spark.submit.deployMode", "client")
    //.set("spark.submit.deployMode", "cluster")
    .set("spark.sql.defaultUrlStreamHandlerFactory.enabled","true")
    .set("spark.serializer",        classOf[KryoSerializer].getName)
    .set("spark.kryo.registrator",  classOf[KryoRegistrator].getName)
    .set("spark.yarn.am.memory", "1024m")
    .set("spark.driver.memory", "2048m")
    .set("spark.executor.memory", "2048m")
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
