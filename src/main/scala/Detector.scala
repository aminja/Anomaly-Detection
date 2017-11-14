import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{window, avg, sum, stddev_samp, udf, max, count}
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.commons.math3.distribution.{TDistribution}


object Detector {

  // Configure to show warnings
  // (lower verbosity than the default)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val log = LogManager.getRootLogger
  log.setLevel(Level.WARN) 

  def main(args: Array[String]): Unit = {

    // Configurations for kafka consumer + spark detection approach
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    val kafkaTopic = sys.env.get("KAFKA_TOPIC")

    // Verify that all settings are set
    require(kafkaBrokers.isDefined, "KAFKA_BROKERS has not been set" +
    "Usage: Detector <bootstrap-servers> <topics>")
    require(kafkaTopic.isDefined, "KAFKA_TOPIC has not been set" +
    "Usage: Detector <bootstrap-servers> <topics>")


    val spark = SparkSession
      .builder
      .appName("DigKalaTask")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers.get)
      .option("subscribe", kafkaTopic.get)
      .option("header", "true")
      .load()
      .selectExpr("split(value, ',')[0] as col1","split(value, ',')[1] as col2")
      .selectExpr("cast (col1 as bigint) time", "cast (col2 as Float) cpu")
      .selectExpr("cast (time / 1000 as timestamp) time", "cast (cpu as Float) cpu")

    // User Defined Function (UDF) -- to apply desired tasks on streams (table)
    // Anomaly (Outlier) Detection Phase
    val status_checker = udf { 
      (x_mean: Float, x_max: Float, x_std: Float, x_num: Float) =>
      val alpha = 0.2
      val z = (x_max - x_mean) / x_std
      val dist = new TDistribution(x_num - 2)
      val t = Math.pow( dist.inverseCumulativeProbability( alpha / x_num ) , 2 )
      val G = ( (x_num-1) / Math.sqrt(x_num) ) * Math.sqrt( t / (x_num-2+t) )
      if (z >= G) "Error"
      else if (z >= 3) "Warning"
      else "Normal"
    }

    // Group data stream into individual windows + Aggregation
    val table = lines.groupBy(
      window($"time", "200 milliseconds", "100 milliseconds")
      ).agg(
          avg($"cpu").alias("mean"),
          max($"cpu").alias("max"),
          stddev_samp($"cpu").alias("std"),
          count($"cpu").alias("#count")
          )
          .withColumn("status", status_checker($"mean", $"max", $"std", $"#count") )
          .orderBy("window")  

    // Configuration of output
    val query = table.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "1000")
      .start()

    query.awaitTermination()
  }

}

