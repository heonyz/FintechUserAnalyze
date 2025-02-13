package commons.utils

import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object DateUtils {

  val TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd")
  val DATEKEY_FORMAT = DateTimeFormat.forPattern("yyyyMMdd")
  val DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm")

  def before(time1:String, time2:String):Boolean = {
    if(TIME_FORMAT.parseDateTime(time1).isBefore(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  def after(time1:String, time2:String):Boolean = {
    if(TIME_FORMAT.parseDateTime(time1).isAfter(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  def minus(time1:String, time2:String): Int = {
    (TIME_FORMAT.parseDateTime(time1).getMillis - TIME_FORMAT.parseDateTime(time2).getMillis)/1000 toInt
  }

  def getDateHour(datetime:String):String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  def getTodayDate():String = {
    DateTime.now().toString(DATE_FORMAT)
  }

  def getYesterdayDate():String = {
    DateTime.now().minusDays(1).toString(DATE_FORMAT)
  }

  def formatDate(date:Date):String = {
    new DateTime(date).toString(DATE_FORMAT)
  }

  def formatTime(date:Date):String = {
    new DateTime(date).toString(TIME_FORMAT)
  }

  def parseTime(time:String):Date = {
    TIME_FORMAT.parseDateTime(time).toDate
  }

  def formatDateKey(date:Date):String = {
    new DateTime(date).toString(DATEKEY_FORMAT)
  }

  def parseDateKey(datekey: String ):Date = {
    DATEKEY_FORMAT.parseDateTime(datekey).toDate
  }

  def formatTimeMinute(date: Date):String = {
    new DateTime(date).toString(DATE_TIME_FORMAT)
  }

}