package analysis.session.Function

import java.util.Date
import analysis._
import analysis.session.bean.SessionAggrStat
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Rand
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.util.Random

object Demand1Function {

  /**
   * 주어진 날짜 범위에 해당하는 사용자 방문 액션 데이터를 RDD로 가져온다.
   *
   * @param taskParam JSON 객체로부터 필요한 파라미터들을 전달받음
   * @return UserVisitAction 객체로 구성된 RDD
   */
  def getActionRddByDateRange(taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from db_userbehaviors.user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"
    val userVisitActionDf: DataFrame = spark.sql(sql)

    import spark.implicits._
    userVisitActionDf.as[UserVisitAction].rdd
  }

  /**
   * 세션 단위로 데이터를 집계하고 사용자 정보와 조인하여 최종 집계 정보를 RDD로 반환한다.
   *
   * @param sessionIdToActionRdd (sessionId, UserVisitAction) 형태의 RDD
   * @return (sessionId, fullAggrInfo) 형태의 RDD
   */
  def aggregateBySession(sessionIdToActionRdd: RDD[(String, UserVisitAction)]): RDD[(String, String)] = {

    val userIdSessionIdToPartAggrInfoRdd = sessionIdToActionRdd.map {
        case (sessionId, userVisitAction) =>
          val userId = userVisitAction.user_id
          val searchKeyword = Option(userVisitAction.search_keyword).getOrElse("")
          val clickCategoryId = Option(userVisitAction.click_category_id).getOrElse(-1L)
          val actionTime = DateUtils.parseTime(userVisitAction.action_time)
          ((userId, sessionId), (Set(searchKeyword), Set(clickCategoryId), actionTime, actionTime, 1))
      }
      .reduceByKey {
        case ((searchSet1, clickSet1, startTime1, endTime1, step1),
        (searchSet2, clickSet2, startTime2, endTime2, step2)) =>
          (
            searchSet1 ++ searchSet2,
            clickSet1 ++ clickSet2,
            if (startTime1.before(startTime2)) startTime1 else startTime2,
            if (endTime1.after(endTime2)) endTime1 else endTime2,
            step1 + step2
          )
      }
      .map { case ((userId, sessionId), (searchSet, clickSet, startTime, endTime, stepLength)) =>
        val searchKeywords = searchSet.filter(_.nonEmpty).mkString(",")
        val clickCategoryIds = clickSet.filter(_ != -1L).mkString(",")
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        val partAggrInfo =
          Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
            Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
            Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
            Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
            Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
            Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, (sessionId, partAggrInfo))
      }

    import spark.implicits._
    val userInfoRdd = spark.sql("SELECT * FROM db_userbehaviors.user_info").as[UserInfo].rdd
    val userInfoMap = userInfoRdd.map(user => (user.user_id, user)).collectAsMap()
    val userInfoBroadcast = spark.sparkContext.broadcast(userInfoMap)

    val sessionIdToFullAggrInfoRdd = userIdSessionIdToPartAggrInfoRdd.flatMap {
      case (userId, (sessionId, partAggrInfo)) =>
        userInfoBroadcast.value.get(userId).map { userInfo =>
          val fullAggrInfo = partAggrInfo + "|" +
            Constants.FIELD_AGE + "=" + userInfo.age +
            "|" + Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional +
            "|" + Constants.FIELD_CITY + "=" + userInfo.city +
            "|" + Constants.FIELD_SEX + "=" + userInfo.sex
          (sessionId, fullAggrInfo)
        }
    }

    sessionIdToFullAggrInfoRdd
  }

  /**
   * 세션 집계 정보에 대해 필터링을 수행하고, 조건에 부합하는 경우 누적 통계(accumulator)를 업데이트한다.
   *
   * @param sessionIdToAggrInfoRdd (sessionId, aggrInfo) 형태의 RDD
   * @param taskParam JSON 객체로부터 필터 조건 파라미터들을 전달받음
   * @param accumulator 통계 정보를 누적하기 위한 Accumulator
   * @return 필터링된 (sessionId, aggrInfo) 형태의 RDD
   */
  def filterSessionAndAggrStat(sessionIdToAggrInfoRdd: RDD[(String, String)],
                               taskParam: JSONObject,
                               accumulator: AccumulatorV2[String, mutable.HashMap[String, Int]]) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var parameter =
      (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
        (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
        (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
        (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
        (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
        (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
        (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")

    if (parameter.endsWith("|")) parameter = parameter.substring(0, parameter.length - 1)
    val filterParameter = parameter

    val filteredSessionIdToAggrInfoRdd = sessionIdToAggrInfoRdd.filter {
      case (sessionId, aggrInfo) =>
        var success = true

        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, filterParameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) success = false

        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, filterParameter, Constants.PARAM_PROFESSIONALS)) success = false

        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, filterParameter, Constants.PARAM_SEX)) success = false

        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, filterParameter, Constants.PARAM_KEYWORDS)) success = false

        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterParameter, Constants.PARAM_CATEGORY_IDS)) success = false

        if (success) {
          accumulator.add(Constants.SESSION_COUNT)

          val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength, accumulator)
          calculateStepLength(stepLength, accumulator)
        }
        success
    }

    filteredSessionIdToAggrInfoRdd
  }
}
