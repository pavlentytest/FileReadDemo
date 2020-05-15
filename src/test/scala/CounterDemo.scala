import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CounterDemo {

  def main(args: Array[String]): Unit = {
  /*  val logFile = "C:/spark/readme.md"
    val conf = new SparkConf().setAppName("CounterDemo").setMaster("local[*]")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("Test app").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numA = logData.filter(line=>line.contains("a")).count()
    val numB = logData.filter(line=>line.contains("b")).count()
    println(s"a: $numA, b: $numB")
    spark.stop()
    */

    val conf = new SparkConf().setAppName("CounterDemo").setMaster("local[*]")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("Test app").getOrCreate()
    val datafile = spark.read
      .format("com.databricks.spark.csv")
      .option("header",true)
      .load("C:/Users/Pavel/Desktop/BigData_plan/BigData/Lab 10/students-performance-in-exams/StudentsPerformance.csv")
    //  datafile.show()
      datafile.createOrReplaceTempView("students")
 //   spark.sql("SELECT COUNT(*) FROM students").show()
    spark.sql("select * from students").show()
    //spark.sql("select `race/ethnicity`, count(`race/ethnicity`) from students group by `race/ethnicity`").show()
    spark.sql("select gender,`race/ethnicity`, `parental level of education`, `math score` from students WHERE `math score` > 75").show()
    spark.stop()


  }

}
