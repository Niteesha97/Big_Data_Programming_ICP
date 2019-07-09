import java.lang.System.setProperty

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Aggregate {
  def main(args: Array[String]) {

    //println("WELCOME")
    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    //val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val file = sqlContext.read.format("com.databricks.spark.csv").load("C:/Users/Niteesha/Desktop/survey.csv")
    val save1 = file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Niteesha/Desktop/saved0003")
    file.registerTempTable("survey")

    val query3 = sqlContext.sql("select COUNT(C1) from survey")
    query3.show()


  }
}