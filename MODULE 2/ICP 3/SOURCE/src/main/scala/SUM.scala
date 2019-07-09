
import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SUM {
  def main(args: Array[String]) {


    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    //val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val file = sqlContext.read.format("com.databricks.spark.csv").load("C:/Users/Niteesha/Desktop/survey1.csv")
    val save1 = file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Niteesha/Desktop/saved0004")
    file.registerTempTable("survey")

    val query4 = sqlContext.sql("select SUM(C1) from survey")
    query4.show()


  }
}