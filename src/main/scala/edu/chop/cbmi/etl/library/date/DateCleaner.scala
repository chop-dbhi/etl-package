package edu.chop.cbmi.etl.library.date

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 7/18/12
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */

class DateCleaner {

  def clean(dateString:String):Option[java.util.Date]   =   {
    val simpleDateRgx = """^([0-9]{1,2})\/([0-9]{1,2})\/([0-9]{2,4})$""".r
    val monthYrRgx = """^([0-9]{1,2})\/([0-9]{2,4})$""".r
    val yrRgx = """^([0-9]{4})$""".r

    val fullDateFormat = new java.text.SimpleDateFormat("MM/dd/yy")
    val monthYrDateFormat = new java.text.SimpleDateFormat("MM/yy")
    val yrDateFormat = new java.text.SimpleDateFormat("yyyy")

    val date = dateString match {
      case simpleDateRgx(mm,dd,yy) => Some(fullDateFormat.parse(dateString))
      case monthYrRgx(mm,yy) => Some(monthYrDateFormat.parse(dateString))
      case yrRgx(yyyy) => Some(yrDateFormat.parse(dateString))
      case _ => None
    }

    date
  }

}
