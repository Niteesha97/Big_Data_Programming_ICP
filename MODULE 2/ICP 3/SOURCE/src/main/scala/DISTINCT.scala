
import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DISTINCT {
  def main(args: Array[String]) {

    //println("WELCOME")
    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    //val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val file = sqlContext.read.format("com.databricks.spark.csv").load("C:/Users/Niteesha/Desktop/survey.csv")
    val save1= file
      .write.format("com.databricks.spark.csv")
      .save("C:/Users/Niteesha/Desktop/saved0006")
    file.registerTempTable("survey")


    val query6 = sqlContext.sql("select COUNT(),C3 from survey GROUP By C3 Having COUNT() > 1")
    query6.show()

    // val query5=sqlContext.sql("select q3.C3,q3.C4,q4.C5 from query3 q3 join query4 q4 on q3.C3=q4.C3")
    //query5.show()
  }
}