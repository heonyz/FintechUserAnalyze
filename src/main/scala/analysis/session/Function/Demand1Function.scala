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
}
