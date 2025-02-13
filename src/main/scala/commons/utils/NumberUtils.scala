package commons.utils

object NumberUtils {

  def formatDouble(num:Double, scale:Int):Double = {
    val bd = BigDecimal(num)
    bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }

}