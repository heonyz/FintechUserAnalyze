package commons.utils

import scala.collection.mutable

object StringUtils {

  def isEmpty(str:String):Boolean = {
    str == null || "".equals(str)
  }

  def isNotEmpty(str:String):Boolean = {
    str != null && !"".equals(str)
  }

  def trimComma(str:String):String = {
    var result = ""
    if(str.startsWith(",")) {
      result = str.substring(1)
    }
    if(str.endsWith(",")) {
      result = str.substring(0, str.length() - 1)
    }
    result
  }

  def fulfuill(str: String):String = {
    if(str.length() == 2) {
      str
    } else {
      "0" + str
    }
  }

  def getFieldFromConcatString(str:String, delimiter:String, field:String):String = {
    try {
      val fields = str.split(delimiter);
      for(concatField <- fields) {
        if(concatField.split("=").length == 2) {
          val fieldName = concatField.split("=")(0)
          val fieldValue = concatField.split("=")(1)
          if(fieldName.equals(field)) {
            return fieldValue
          }
        }
      }
    } catch{
      case e:Exception => e.printStackTrace()
    }
    null
  }

  def setFieldInConcatString(str:String, delimiter:String, field:String, newFieldValue:String):String = {

    val fieldsMap = new mutable.HashMap[String,String]()

    for(fileds <- str.split(delimiter)){
      var arra = fileds.split("=")
      if(arra(0).compareTo(field) == 0)
        fieldsMap += (field -> newFieldValue)
      else
        fieldsMap += (arra(0) -> arra(1))
    }
    fieldsMap.map(item=> item._1 + "=" + item._2).mkString(delimiter)
  }

}