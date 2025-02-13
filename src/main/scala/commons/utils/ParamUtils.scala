package commons.utils

import net.sf.json.JSONObject

object ParamUtils {

  def getParam(jsonObject:JSONObject, field:String):String = {
    jsonObject.getString(field)
  }

}