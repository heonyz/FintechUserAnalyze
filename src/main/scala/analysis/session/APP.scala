package analysis.session
import analysis._
import analysis.session.Function.UserSessionAnalysisFunction1


object APP {
  def main(args: Array[String]): Unit = {

    UserSessionAnalysisFunction1.Demand1()

    println(s"Spark App Name: ${spark.sparkContext.appName}")

    println(s"Task Parameters: ${taskParam}")

    println(s"Task UUID: $taskUUID")

  }
}
