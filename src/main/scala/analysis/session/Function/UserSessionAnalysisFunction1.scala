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


    println("2단계: SessionId 기반으로 변환 시작")
    val SessionId2ActionRDD = actionRDD.map(obj => {
      (obj.session_id, obj)
    })
    println(s"2단계 완료: SessionId2ActionRDD 개수 = ${SessionId2ActionRDD.count()}")
    SessionId2ActionRDD.take(10).foreach(println) // 샘플 데이터 출력


    println("3단계: Session 집계 정보 생성 시작")
    val session_id2AggrInfoRDD = Demand1Function.aggregateBySession(SessionId2ActionRDD)
    println(s"3단계 완료: session_id2AggrInfoRDD 개수 = ${session_id2AggrInfoRDD.count()}")
    session_id2AggrInfoRDD.take(10).foreach(println) // 샘플 데이터 출력

}
