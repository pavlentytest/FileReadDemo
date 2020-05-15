import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CounterDemo {

  def main(args: Array[String]): Unit = {
    val logFile = "C:/spark/readme.md"
    val conf = new SparkConf().setAppName("CounterDemo").setMaster("local[*]")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("Test app").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numA = logData.filter(line=>line.contains("a")).count()
    val numB = logData.filter(line=>line.contains("b")).count()
    println(s"a: $numA, b: $numB")
    spark.stop()
  }

}
