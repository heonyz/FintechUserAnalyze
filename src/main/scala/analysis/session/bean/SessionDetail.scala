package analysis.session.bean

case class SessionDetail(taskid:String,
                         userid:Long,
                         sessionid:String,
                         pageid:Long,
                         actionTime:String,
                         searchKeyword:String,
                         clickCategoryId:Long,
                         clickProductId:Long,
                         orderCategoryIds:String,
                         orderProductIds:String,
                         payCategoryIds:String,
                         payProductIds:String)