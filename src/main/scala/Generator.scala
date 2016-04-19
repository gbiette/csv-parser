import java.io.{FileOutputStream, BufferedWriter, OutputStreamWriter, File}

import org.joda.time.DateTime
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
 * Created by Gregoire on 13/04/2016.
 */
object Generator {
  def main(args: Array[String]): Unit = {

    val mapArgs = args.map { arg =>
      val tab = arg.split("=")
      tab(0) -> tab(1)
    }.toMap

    //    val variables = Array[(String, String)](("varString", "String"), ("varInt", "Int"), ("varDouble", "Double"))
    val variables = getVariables(mapArgs.getOrElse("name", "Salut"), mapArgs.getOrElse("dlm", ";"), mapArgs.getOrElse("filePath", ""))
    generate(mapArgs, variables)

  }

  def getVariables(name: String, dlm: String, path: String): Array[(String, String)] = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    val conf: SparkConf = new SparkConf()
    conf.setAppName(s"generator_$name")
    conf.setMaster("local[1]")

    val sc: SparkContext = new SparkContext(conf)

    val allLines = sc.textFile(path)
    val header = allLines.first()

    val data = allLines.filter(_ != header).map(_.split(dlm, -1).map(_.trim))

    val varNames = header.split(dlm, -1).map(_.trim).zipWithIndex.map { case (name, idx) =>
      if (name == "") "col" + idx else name
    }

    val varTypes = data.map {
      _.map(detectType)
    }.reduce(multArrays).map(getType)

    sc.stop()

    varNames.zip(varTypes)
  }

  def detectType(input: String): Int = {
    if (input == "") {
      1
    }
    try {
      val get = input.toInt
      1
    }
    catch {
      case e: Exception => {
        try {
          val get = input.toDouble
          2
        }
        catch {
          case e: Exception =>
            0
        }
      }
    }
  }

  def getType(code: Int): String = {
    println(code)
    if (code == 1)
      "Int"
    else if (code == 0)
      "String"
    else
      "Double"
  }

  def multArrays(a: Array[Int], b: Array[Int]): Array[Int] = {
    a.zip(b).map { case (x, y) =>
      val xred = if (x > Int.MaxValue / 4) 2 else x
      val yred = if (x > Int.MaxValue / 4) 2 else y
      xred * yred
    }
  }


  def generate(args: Map[String, String], variables: Array[(String, String)]): Unit = {

    val name = args.getOrElse("name", "Coucou")

    val writerParser = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("src/main/scala/generated/parser/" + name + "Parser.scala"), "utf-8"))

    val date = DateTime.now().toString("yyyy/MM/dd")
    val className = name + "Parser"
    val start = s"""package generated.parser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import util.CustomUtils._
import org.elasticsearch.spark._
import generated.struct.$name
/**
 * Generated on $date
 */
object $className {
"""

    val escluster = args.getOrElse("escluster", "elasticsearch")
    val esindex = args.getOrElse("esindex", "index_" + name)
    val esRep = args.getOrElse("esRep", "1")
    val esShards = args.getOrElse("esShards", "5")
    val esDelete = args.getOrElse("esDelete", "true")
    val filePath = args.getOrElse("filePath", "")
    val dlm = args.getOrElse("dlm", ";")
    val options =
      s"""  val mappingFile = "/ressource/mapping_$name.json"
  val es_cluster = "$escluster"
  val es_index = "$esindex"
  val nbReplicas = $esRep
  val nbShards = $esShards
  val deleteIfExists = $esDelete

  val pathFile = "$filePath"
  val dlm = "$dlm"
"""

    val run =
      s"""
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf()
    conf.setAppName("parser_$name")

    val sc: SparkContext = new SparkContext(conf)

  }

  def run()(implicit sc: SparkContext): Unit = {

    val es_type = prepareIndex()

    val data = sc.textFile(pathFile).map(s => $name.createCaseClass(s, dlm))
    val json = data.map(_.toJson)
    json.saveJsonToEs(es_index + "/" + es_type)
  }

  def prepareIndex(): String = {
    val (es_type, mapping) = loadMapping(mappingFile)
    initiateIndex(es_cluster, es_index, es_type, mapping, nbReplicas, nbShards, deleteIfExists)
    es_type
  }
"""

    val end = "}"


    writerParser.write(start)
    writerParser.write(options)
    writerParser.write(run)
    writerParser.write(end)

    writerParser.flush()
    writerParser.close()


    val writerStruct = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("src/main/scala/generated/struct/" + name + ".scala"), "utf-8"))

    val classArgs = variables.map { case (varname, vartype) =>
      s"""val $varname: $vartype"""
    }.mkString(", ")

    val jsonPart = variables.map { case (varname, vartype) =>
      if (vartype == "String") {
        "str += \"\\\"" + varname + "\\\":\\\"\" + " + varname + " + \"\\\"\""
      } else {
        "str += \"\\\"" + varname + "\\\":\" + " + varname
      }
    }.mkString(" + \",\"\n    ")

    val factoryPart = variables.zipWithIndex.map { case ((varname, vartype), idx) =>
      if (vartype == "String") {
        s"tab($idx)"
      } else {
        s"tab($idx).to$vartype"
      }
    }.mkString(", ")

    val caseClass =
      s"""package generated.struct

/**
 * Generated on $date
 */
case class $name($classArgs) {
  def toJson: String = {
    var str = "{"
    $jsonPart
    str += "}"
    str
  }
}

object $name {
  def createCaseClass(toParse: String, dlm: String): $name = {
    val tab = toParse.split(dlm)
    $name($factoryPart)
  }
}
"""

    writerStruct.write(caseClass)
    writerStruct.flush()
    writerStruct.close()

    val writerMapping = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("src/main/resources/mapping_" + name + ".json"), "utf-8"))

    val mappingStart =
      s"""{
  "$name": {
    "properties": {
"""

    val mappingPart = variables.map { case (varname, vartype) =>
      if (vartype == "Int") {
        s"""      "$varname": {
        "type": "long"
      }"""
      }
      else if (vartype == "Double") {
        s"""      "$varname": {
        "type": "double"
      }"""
      } else {
        s"""      "$varname": {
        "type": "string",
        "index": "not_analyzed"
      }"""
      }
    }.mkString(",\n")

    val mappingEnd =
      """
    }
  }
}"""

    writerMapping.write(mappingStart)
    writerMapping.write(mappingPart)
    writerMapping.write(mappingEnd)

    writerMapping.flush()
    writerMapping.close()

  }
}
