// $example on$
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.rdd._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
// $example off:schema_inferring$
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.lang.Object
import scala.collection.mutable.ArrayBuffer

/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object PageRankExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName("PageRankGraphx").config("spark.driver.memory","8g")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","hdfs://10.254.0.33:8020/user/ubuntu/applicationHistory")
      .config("spark.executor.memory","8g")
      .config("spark.executor.cores","4")
      .config("spark.task.cpus","1")
      .config("spark.executor.instances","4")
      .config("spark.default.parallelism","16")
      .master("spark://10.254.0.33:7077")
      .getOrCreate()

    val path = new Path(args(1))
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val stream = fileSystem.open(path)



    val sc = spark.sparkContext
    val vfile = sc.textFile(args(1))

    val array = vfile.collect.toList
    println(array)
    
    var count1 = 0
    var edgeString = ""
    var edgesList = ArrayBuffer[Edge[VertexId]]()
    for ( l1<- array ){
      var count2 = 0
      for ( l2<-array){
        if(count1==count2){
          //do nothing
        } else {
          val splitWords1 = l1.split("\\s+")
          val splitWords2 = l2.split("\\s+")
          val intersection = splitWords1.intersect(splitWords2)
          if(intersection.length!=0) {
            if(splitWords1.length>splitWords2.length)
            {
              edgesList += Edge(count1, count2, 1L)
            }
            else
              edgesList += Edge(count1, count2, 2L)
          }
        }
        count2=count2+1

    }
    count1= count1+1
  }

    val edgesRDDFromList = sc.parallelize(edgeString.split("\n"))
    val vRDD: RDD[(VertexId, Array[String])] = vfile.map(line => line.split("\\s+")).zipWithIndex().map(line => (line._2, line._1))
    val eRDD: RDD[Edge[VertexId]] = sc.parallelize(edgesList)
    val g: Graph[Array[String], VertexId] = Graph(vRDD, eRDD)
    println("***Number of Edges where the number of words in the source vertex is strictly larger than the number of words in destination vertex :"+g.edges.filter{ case Edge(src, dst, prop) => prop == 1 }.count)
    spark.stop()
    }
}

// scalastyle:on println
