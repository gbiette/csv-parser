package util

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.{Node, NodeBuilder}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by gregoire on 18/01/16.
 */
object CustomUtils {

  def loadPathAndLine(sc: SparkContext, path: String): RDD[(String, String)] = {
    val data = sc.hadoopFile(path + "/*", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.defaultMinPartitions)

    val hadoopRdd = data.asInstanceOf[HadoopRDD[LongWritable, Text]]

    hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map { tpl ⇒ (file.getPath, tpl._2) }
    }.map { case (path, line) => (path.toString, line.toString) }
  }

  def addArray(a: Array[Double], b: Array[Double]): Array[Double] = {
    a.zip(b).map(s => s._1 + s._2)
  }

  def loadMapping(path: String): (String, String) = {
    val stream = getClass.getResourceAsStream(path)
    val mapping = scala.io.Source.fromInputStream(stream).getLines.mkString("\n")
    val esType = mapping.replaceAll("(\n| )", "").split("\"")(1)
    (esType, mapping)
  }


  /**
   * Convert timestamp in string to a date time object.
   * @param time  to convert
   * @param format  format of timestamp, by default format_date_out
   * @return
   */
  def toDT(time: String, format: String = "yyyy/MM/dd HH:mm:ss"): DateTime = {
    DateTimeFormat.forPattern(format).parseDateTime(time)
  }

  def dtToString(time: DateTime, format: String = "yyyy/MM/dd HH:mm:ss"): String = {
    time.toString(format)
  }


  def initiateIndex(cluster_name: String, index_name: String, es_type: String, mapping: String, nbShards: Int = 5, nbReplicas: Int = 1, deleteIfExists: Boolean = true): Unit = {
    val esSettings: ImmutableSettings.Builder = ImmutableSettings.settingsBuilder()
    esSettings.put("number_of_shards", nbShards.toString)
    esSettings.put("number_of_replicas", nbReplicas.toString)
    val esNode: Node = NodeBuilder.nodeBuilder().clusterName(cluster_name).client(true).node()
    val esClient: Client = esNode.client()

    val indexExists: Boolean = esClient.admin().indices().exists(new IndicesExistsRequest(index_name)).actionGet().isExists
    if (indexExists && deleteIfExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(index_name)).actionGet()
      println("--- Index " + index_name + " deleted")
      createIndex(esClient, index_name, esSettings, es_type, mapping)
      println("--- Index " + index_name + " created")
    }
    else if (!indexExists) {
      createIndex(esClient, index_name, esSettings, es_type, mapping)
      println("--- Index " + index_name + " created")
    }
    esNode.close()
  }


  private def createIndex(esClient: Client, es_index_name: String, es_settings: ImmutableSettings.Builder, es_type: String, mapping: String): Unit = {
    val esCreateIndex: CreateIndexRequest = new CreateIndexRequest(es_index_name)
    esCreateIndex.settings(es_settings)
    esCreateIndex.mapping(es_type, mapping)
    esClient.admin().indices().create(esCreateIndex).actionGet()
  }


}
