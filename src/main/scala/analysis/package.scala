import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import net.sf.json.JSONObject
import java.util.UUID


package object analysis {

  lazy val conf: SparkConf = new SparkConf()
    .setAppName("SessionAnalysis")
    .setMaster("local[*]")
    .set("spark.sql.caseSensitive", "false")
    .set("spark.executor.extraClassPath", "mysql-connector-j-8.0.33.jar")
    .set("spark.driver.extraClassPath", "mysql-connector-j-8.0.33.jar")

  lazy val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .config("spark.sql.caseSensitive", "false")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/app/spark-events")
    .config("spark.metrics.conf", "/opt/spark/conf/metrics.properties")
    //    .config("hive.metastore.uris", "thrift://hive:9083") // Hive Metastore URL
    .enableHiveSupport()
    .getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext

  lazy val taskParam: JSONObject = {
    val jsonStr = Option(ConfigurationManager.config.getString(Constants.TASK_PARAMS))
      .getOrElse(throw new RuntimeException("TASK_PARAMS not found in configuration"))
    JSONObject.fromObject(jsonStr)
  }

  lazy val taskUUID: String = UUID.randomUUID().toString
}
