import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Vasanthi_Mudunuri_Program_1 {
  def main(args: Array[String]) {
      val conf=new SparkConf().setAppName("Standard Deviation").setMaster("local")
      val sc=new SparkContext(conf)
      val inputpath="hdfs://hadoop1:9000/"+args(0) //inputpath for file
      val outputpath="hdfs://hadoop1:9000/"+args(1) //outputpath for file
      val lines: RDD[String] = sc.textFile(inputpath)  //Reading file and creating RDD
      val pairs =lines.map(x =>(x.split("::")(1),x.split("::")(2)))     //splitting the string and getting required key and values
      val result=pairs.groupByKey.mapValues { values => //calculating standard deviation
      val ratings = values.map(_.toInt)
      val n = ratings.count(x=>true)
      val sum = ratings.sum 
      val sumSquares = ratings.map(x => x * x).sum 
      math.sqrt(n * sumSquares - sum * sum) / n }
      result.saveAsTextFile(outputpath) //saving file in outputpath
   } 	
}
