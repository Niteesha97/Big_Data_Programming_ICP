
import java.lang.System.setProperty

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Query3 {
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
      .save("C:/Users/Niteesha/Desktop/saved001")
    file.registerTempTable("survey")


    file.registerTempTable("survey")
    file.registerTempTable("survey1")
    val query3 = sqlContext.sql("select s1.C3,s2.C4 from survey s1 join survey1 s2 on s1.C3=s2.C3")
    // val query4 = sqlContext.sql("select C3,C4,C5,C13 from survey where C13 like '%No'")

    query3.show()
    query3.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputfile3")

    // val query5=sqlContext.sql("select q3.C3,q3.C4,q4.C5 from query3 q3 join query4 q4 on q3.C3=q4.C3")
    //query5.show()
  }
}