package simplesearch

object SingleLogging{
    val INFO = "INFO"
    val ERROR = "ERROR"
    val DEBUG = "DEBUG"
    val METRIC = "METRIC"

    def log(tag: String, msg: String): Unit ={
      println(s"[${tag}]: ${msg}")
    }

    def log_debug(msg: String){
      println(s"[${DEBUG}]: ${msg}")
    }

    def log_info(msg: String){
      println(s"[${INFO}]: ${msg}")
    }

    def log_error(msg: String){
      println(s"[${ERROR}]: ${msg}")
    }

    def log_metric(label:String, msg: String){
      println(s"[${METRIC}], ${label}, ${msg}")
    }
}
