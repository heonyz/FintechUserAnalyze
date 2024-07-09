package commons.utils

object ValidUtils {

  def between(data:String, dataField:String, parameter:String, startParamField:String, endParamField:String):Boolean = {
    var flag = false

    val startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField)
    val endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField)
    if(startParamFieldStr == null || endParamFieldStr == null) {
      flag = true
      return flag
    }

    val startParamFieldValue = startParamFieldStr.toInt
    val endParamFieldValue = endParamFieldStr.toInt

    val dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldStr != null) {
      val dataFieldValue = dataFieldStr.toInt
      if(dataFieldValue >= startParamFieldValue && dataFieldValue <= endParamFieldValue) {
        flag = true
        return flag
      } else {
        return flag
      }
    }
    flag
  }

  def in(data:String, dataField:String, parameter:String, paramField:String):Boolean = {
    var flag = true

    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if(paramFieldValue == null) {
      return flag
    }
    val paramFieldValueSplited = paramFieldValue.split(",")

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldValue != null && dataFieldValue != "-1") {
      val dataFieldValueSplited = dataFieldValue.split(",")

      for(singleDataFieldValue <- dataFieldValueSplited) {
        for(singleParamFieldValue <- paramFieldValueSplited) {
          if(singleDataFieldValue.compareTo(singleParamFieldValue) ==0) {
            return flag
          }
        }
      }
    }
    flag
  }

  def equal(data:String, dataField:String, parameter:String, paramField:String):Boolean = {
    var flag = true

    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if(paramFieldValue == null) {
      flag = true
      return flag
    }else {
      flag = false
    }

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldValue != null) {
      if(dataFieldValue.compareTo(paramFieldValue) == 0) {
        flag = true
        return flag
      }else{
        flag = false
      }
    }
    flag
  }

}