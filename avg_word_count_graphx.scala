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
    
    def avg(a: (VertexId, (Int, Int))): (VertexId, Float) = {
      val minus1 = -1
      if(a._2._1==0){
        return (a._1, minus1.toFloat)
      }
      val average = a._2._2.toFloat/a._2._1
      return (a._1, average)
    }


    // $example off$
    val popularVertices: VertexRDD[(Int, Int)] = g.aggregateMessages[(Int, Int)](
      triplet => { // Map Function
        triplet.sendToSrc(1, triplet.dstAttr.length)
      },
      (a, b) => (a._1+b._1, a._2+b._2) // Reduce Function
    )

    //println("***Count of popularvertices : "+popularVertices.count())

   /* val facts: RDD[String] =
  g.triplets.map(triplet =>
    triplet.srcId + " with length "+triplet.srcAttr.length+" is the " + triplet.attr + " of " + triplet.dstId +" with length "+triplet.dstAttr.length)
facts.collect.foreach(println(_))*/

    val popularVertex: RDD[(VertexId, Float)] = popularVertices.map(vertex=>avg(vertex))
    //val popularVerticesPrintRDD = popularVertices.map(vertex=>"****VertexID "+vertex._1+" has "+vertex._2._1+" neighbors and number of words "+vertex._2._2)
    //popularVerticesPrintRDD.collect.foreach(println(_))
    val popularVertexPrintRDD = popularVertex.map(vertex=>"****Average Number of Words in Neighborhood of Vertex "+vertex._1+" is "+vertex._2)
    popularVertexPrintRDD.collect.foreach(println(_))

    spark.stop()
    }
}

// scalastyle:on println
