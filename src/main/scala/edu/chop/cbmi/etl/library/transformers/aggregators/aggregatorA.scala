package edu.chop.cbmi.etl.library.transformers.aggregators

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 3/7/12
 * Time: 5:43 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.chop.cbmi.dataExpress.dataModels.DataRow
import scala.util.control.Breaks._

case class aggregatorA(aggregators: Seq[String], rowToAggregate: String) {


  //  Check newAggregation
  //  If false then aggregation is not complete
  //  If true then aggregation is complete
  //  Aggregation will have the value of the completed aggregation
  //  StopAggregation will effectively reset the aggregator
  //    1.  Set aggregation to current aggregation
  //    2.  set new Aggregation to false
  //    3.  Clear current Aggregation

  var previousRow:                DataRow[Any]      = DataRow.empty
  
  var initialize: Boolean                           = true

  //The received row is for a new aggregation
  var newAggregation: Boolean                       = false

  private var aggregation:String                    = ""

  private var currentAggregation: String            = ""

  def aggregate(row: DataRow[_])  : Boolean  = {

    breakable {
      for (i<- (0 to (aggregators.length - 1) ))  {
        if (initialize) {

          //newAggregation  = true

          if (initialize){  initialize  = false}
          break
        }
        else  {
          if (row(aggregators(i)).getOrElse(null)  != previousRow(aggregators(i)).getOrElse(null)    )  {
            //print(row(aggregators(i)).getOrElse(null) + "   " + previousRow(aggregators(i)).getOrElse(null) + "\n")
            newAggregation  = true

            break

          }
          else  {

            //If we have compared all the aggregators
            if (  i == (aggregators.length - 1) ) {
              newAggregation  = false
              break

            }
          }
        }

      }
    }
    
    if (!(newAggregation) ) {

      currentAggregation = currentAggregation + row(rowToAggregate).getOrElse("").toString

      previousRow = row

      newAggregation
    }
    else  {
    // Done Previous Aggregation
    // A new aggregation has been started
    //

      aggregation         = currentAggregation

      currentAggregation  = ""

      currentAggregation  = row(rowToAggregate).getOrElse("").toString

      previousRow         = row

      newAggregation
    }

  }


  def stopAggregation() {

      aggregation             = currentAggregation
      
      newAggregation          = false
      
      currentAggregation      = ""
  }


  def getAggregation: String  =  {
  aggregation
  }







}