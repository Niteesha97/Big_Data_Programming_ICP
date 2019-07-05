
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Frequency {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input =  sc.textFile("input.txt")

    val output = "op3"

    val words = input.flatMap(line => line.split(""))

    words.foreach(f=>println(f))

    val c = words.map(words => (words, 1)).reduceByKey(_+_,1)

    val wrdList=c.sortBy(outputLIst=>outputLIst._1,ascending = true)

    wrdList.foreach(outputLIst=>println(outputLIst))

    wrdList.saveAsTextFile(output)

    wrdList.take(10).foreach(outputLIst=>println(outputLIst))

    sc.stop()

  }

}