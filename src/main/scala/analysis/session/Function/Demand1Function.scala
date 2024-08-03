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
}
