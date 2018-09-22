import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Vasanthi_Mudunuri_Program_2 {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("Movie AverageRatings").setMaster("local")
    val sc=new SparkContext(conf)
    val inputpath="hdfs://hadoop1:9000/"+args(0) //inputpath for file
    val outputpath="hdfs://hadoop1:9000/"+args(1) //outputpath for file
    val lines: RDD[String] = sc.textFile(inputpath) //reading file and creating RDD
    val givenMovieId=args(2) 
    val pairs =lines.map(x =>(x.split("::")(1),x.split("::")(2).toInt)) //splitting string and getting required key and values
    val result =pairs.filter{case(key,value) => key == givenMovieId} //calculating average ratings for given movieid
    val result1 = result.reduceByKey((x,y)=>x+y)
    val result2 = pairs.combineByKey((v)=>(v,1),
      (acc:(Int,Int),v) => (acc._1+v, acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)).map{
      case(key,value) => (key,value._2.toDouble)
    }.filter{case(key,value) => key == givenMovieId}
    val result3 = result1.join(result2)
    val finalResult = result3.map(x=>(x._1,x._2._1/x._2._2))
    finalResult.saveAsTextFile(outputpath) //saving output in file 
  }
}

