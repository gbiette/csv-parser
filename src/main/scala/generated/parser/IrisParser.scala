package generated.parser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import util.CustomUtils._
import org.elasticsearch.spark._
import generated.struct.Iris
/**
 * Generated on 2016/04/13
 */
object IrisParser {
  val mappingFile = "/ressource/mapping_Iris.json"
  val es_cluster = "test"
  val es_index = "index_Iris"
  val nbReplicas = 1
  val nbShards = 5
  val deleteIfExists = true

  val pathFile = ""
  val dlm = ","

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf()
    conf.setAppName("parser_Iris")

    val sc: SparkContext = new SparkContext(conf)

  }

  def run()(implicit sc: SparkContext): Unit = {

    val es_type = prepareIndex()

    val data = sc.textFile(pathFile).map(s => Iris.createCaseClass(s, dlm))
    val json = data.map(_.toJson)
    json.saveJsonToEs(es_index + "/" + es_type)
  }

  def prepareIndex(): String = {
    val (es_type, mapping) = loadMapping(mappingFile)
    initiateIndex(es_cluster, es_index, es_type, mapping, nbReplicas, nbShards, deleteIfExists)
    es_type
  }
}