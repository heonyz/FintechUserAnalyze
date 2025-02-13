package analysis.session.Function

import analysis.session.accumulator.SessionAggrStatAccumulator
import analysis._

object UserSessionAnalysisFunction1 {

  def Demand1() = {

    println("==== Demand1 시작 ====")

    println("1단계: 데이터 읽기 및 RDD 생성 시작 ")
    val actionRDD = Demand1Function.getActionRDDByDateRange(taskParam)
    println(s"1단계 완료: Action RDD 개수 = ${actionRDD.count()}")
    actionRDD.take(10).foreach(println)

}
