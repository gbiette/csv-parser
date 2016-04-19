package generated.struct

case class Iris(val sepal_length: Double, val sepal_width: Double, val petal_length: Double, val petal_width: Double, val species: String) {
  def toJson: String = {
    var str = "{"
    str += "\"sepal_length\":" + sepal_length + ","
    str += "\"sepal_width\":" + sepal_width + ","
    str += "\"petal_length\":" + petal_length + ","
    str += "\"petal_width\":" + petal_width + ","
    str += "\"species\":\"" + species + "\""
    str += "}"
    str
  }
}

object Iris {
  def createCaseClass(toParse: String, dlm: String): Iris = {
    val tab = toParse.split(dlm)
    Iris(tab(0).toDouble, tab(1).toDouble, tab(2).toDouble, tab(3).toDouble, tab(4))
  }
}
