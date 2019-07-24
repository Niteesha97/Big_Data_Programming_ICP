
import org.apache.spark.{SparkConf, SparkContext}
object BFS {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type V = Int
    type Graph = Map[V, List[V]]
    val g: Graph = Map(1 -> List(1,2,3), 2 -> List(3,4,6), 3 -> List(1,3), 4 -> List(4,5),6 -> List(2,4),5 -> List(2,6))
    def BFS(start: V, g: Graph): List[List[V]] = {
      def BFS0(elems: List[V],visited: List[List[V]]): List[List[V]] = {
        val newmembers = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newmembers.isEmpty)
          visited
        else
          BFS0(newmembers, newmembers :: visited)
      }
      BFS0(List(start),List(List(start))).reverse
    }
    val result=BFS(1,g )
    println(result.mkString(","))

  }
}