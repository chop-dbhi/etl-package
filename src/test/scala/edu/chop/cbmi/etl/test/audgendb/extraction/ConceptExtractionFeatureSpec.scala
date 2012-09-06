package edu.chop.cbmi.etl.test.audgendb.extraction

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 11/22/11
 * Time: 1:22 PM
 * To change this template use File | Settings | File Templates.
 */

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FeatureSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen


import edu.chop.cbmi.etl.audgendb.extraction.ConceptExtraction


@RunWith(classOf[JUnitRunner])
class ConceptExtractionFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers {


  ignore("aggregate:"                      )   {

    scenario("The user can aggregate an unaggregated concept table ") {

      val extraction =  ConceptExtraction()

      try {
      extraction.aggregate(Option("concept_aggregate_test_01"))
      }
      catch {
      case  e:RuntimeException  => throw e
      }


    }



  }




}
