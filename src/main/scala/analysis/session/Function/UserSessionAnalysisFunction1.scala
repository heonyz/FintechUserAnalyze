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


    println("4단계: 누적기 등록 시작")
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    sc.register(sessionAggrStatAccumulator)
    println("4단계 완료: 누적기 등록 완료")


    println("5단계: 필터링 및 누적기 업데이트 시작")
    val filterSession_id2AggrInfoRDD = Demand1Function.filterSessionAndAggrStat(
      session_id2AggrInfoRDD,
      taskParam,
      sessionAggrStatAccumulator
    )
    val filteredRDDCount = filterSession_id2AggrInfoRDD.count()
    println(s"5단계 완료: Filter된 RDD 개수 = $filteredRDDCount")
    filterSession_id2AggrInfoRDD.take(10).foreach(println) // 샘플 데이터 출력
    println("5단계 완료: 누적기 값 상세 출력")
    sessionAggrStatAccumulator.value.foreach { case (key, value) =>
      println(s"  $key -> $value")
    }


    println("6단계: 상세 정보와 사용자 행동 데이터 집계 시작" )
    val session_idDetailRDD = Demand1Function.getSession_id2detailRDD(filterSession_id2AggrInfoRDD, SessionId2ActionRDD)
    println(s"6단계 완료: session_idDetailRDD 개수 = ${session_idDetailRDD.count()}")
    session_idDetailRDD.take(10).foreach(println) // 샘플 데이터 출력
  }
}
