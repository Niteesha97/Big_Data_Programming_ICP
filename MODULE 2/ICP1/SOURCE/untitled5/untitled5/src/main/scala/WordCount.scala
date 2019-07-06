
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input =  sc.textFile("ip.txt")

    val output = "op1"

    val wrds = input.flatMap(line => line.split("\\W+"))

    wrds.foreach(f=>println(f))

    val counts = wrds.map(words => (words, 1)).reduceByKey(_+_,1)

    val wrdList=counts.sortBy(outputLIst=>outputLIst._1,ascending = true)

    wrdList.foreach(outputLIst=>println(outputLIst))

    wrdList.saveAsTextFile(output)

    wrdList.take(10).foreach(outputLIst=>println(outputLIst))

    sc.stop()

  }

}