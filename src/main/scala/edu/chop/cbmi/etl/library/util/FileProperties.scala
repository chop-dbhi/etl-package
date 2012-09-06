package edu.chop.cbmi.etl.util


import java.util.Properties


/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 02/14/12
 * Time: 2:25 PM
 * To change this template use File | Settings | File Templates.
 */

case class FileProperties(sourcePropertiesFilePath: String) {

  val prop_path = sourcePropertiesFilePath
  val inputStream:java.io.FileInputStream = new java.io.FileInputStream(prop_path)
  val props = new Properties()
  props.load(inputStream)
  inputStream.close()


}