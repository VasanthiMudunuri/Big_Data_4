import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Vasanthi_Mudunuri_Program_3 {
  def toInteger(s: String): Int = {
  try {
    s.toInt
  } catch {
    case e: Exception => 0
  }
}
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("ACSandCFS Join").setMaster("local")
    val sc=new SparkContext(conf)
    val inputpath1="hdfs://hadoop1:9000"+args(0) //inputpath for file
    val inputpath2="hdfs://hadoop1:9000"+args(1)  //outputpath for file
    val lines: RDD[String] = sc.textFile(inputpath1) //reading file and creating RDD
    val header=lines.first();
    val pairs =lines.filter { row => row != header }.map(x =>(x.split(",")(2),x.split(",")(3)))
    val lines1: RDD[String] = sc.textFile(inputpath2)
    val header1=lines1.first();
    val pairs1 =lines1.filter { row => row != header1 }.map(x =>(x.split(",")(2),toInteger(x.split(",")(8)))).filter{case (key,value)=> value>0}
    val sumoftons=pairs1.reduceByKey((x,y)=>x+y)
    val joindata: RDD[(String,String)]=pairs.join(sumoftons).map{x=>(x._1,x._2._1+x._2._2.toString())}
    joindata.saveAsSequenceFile("hdfs://hadoop1:9000"+args(2)) //saving output as sequence file
  }
}
