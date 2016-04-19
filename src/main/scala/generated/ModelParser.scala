package generated

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import util.CustomUtils._
import org.elasticsearch.spark._

/**
 * Created by Gregoire on 11/04/2016.
 */
object ModelParser {

  //TODO : GENERATE
  val mappingFile = "/ressource/mapping" + "_model" + ".json"
  val es_cluster = "elasticsearch"
  //TODO : GENERATE
  val es_index = "index" + "_model"
  val nbReplicas = 1
  val nbShards = 5
  val deleteIfExists = true

  val pathFile = ""
  val dlm = ";"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf()
    //TODO : GENERATE
    conf.setAppName("parser" + "_model")

    val sc: SparkContext = new SparkContext(conf)


  }

  def run()(implicit sc: SparkContext): Unit = {

    val es_type = prepareIndex()

    val data = sc.textFile(pathFile).map(s => Model.createCaseClass(s, dlm))
    val json = data.map(_.toJson)
    json.saveJsonToEs(es_index + "/" + es_type)
  }

  def prepareIndex(): String = {
    val (es_type, mapping) = loadMapping(mappingFile)
    initiateIndex(es_cluster, es_index, es_type, mapping, nbReplicas, nbShards, deleteIfExists)
    es_type
  }

  //TODO : GENERATE
  case class Model(val varInt: Int, val varString: String, val varDouble: Double) {
    def toJson: String = {
      var str = "{"
      str += "\"varInt\":" + varInt + ","
      str += "\"varString\":\"" + varString + "\","
      str += "\"varDouble\":" + varDouble
      str += "}"
      str
    }
  }

  //TODO : GENERATE
  object Model {
    def createCaseClass(toParse: String, dlm: String): Model = {
      val tab = toParse.split(dlm)
      Model(tab(0).toInt, tab(1), tab(2).toDouble)
    }
  }


}
