package myproj.techtest

import java.util.Properties
import scala.language.implicitConversions
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object DETechTest extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: Inputdata file path , output data directory , input file delimiter")
      System.exit(100)
    }

    logger.info("Starting  Spark")
    // spark session created with local mode

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()
    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    val inputDataFile = args(0)
    val outputPath = args(1)
    val delim = args(2)
    val schemaDefn = StructType(Array(
      StructField("column1", IntegerType, true),
      StructField("column2", IntegerType, true)
    ))
    // create the source Dataframe from input file
    val sourceDf = spark.read.schema(schemaDefn).option("delimiter", delim).option("header", "true").csv(inputDataFile)
    // fill the null / na with 0
    val nullDf = sourceDf.na.fill(0)
    nullDf.createOrReplaceTempView("oddSeg")
    // business logic as seggregate the odd values
    val oddSegDf = spark.sql("select column1,column2 , count(column2) as cntColumn2 from oddSeg group by column1 , column2 ")
    oddSegDf.createOrReplaceTempView("tempTable")
    val tempTableDf = spark.sql("select * , case when cntColumn2 % 2 = 0 then 'even' else 'odd' end as oddColumn from tempTable ")
    tempTableDf.createOrReplaceTempView("resultTable")
    val resultTableDf = spark.sql("select column1,column2  from resultTable where  oddColumn = 'odd' ")
    resultTableDf.createOrReplaceTempView("errorCheck")
    val errorCheckDfTemp = spark.sql("select column1 ,count(1) as errorCnt from errorCheck group by column1")
    errorCheckDfTemp.createOrReplaceTempView("finalErrorCheck")
    val finalErrorCheck = spark.sql("select max(errorCnt) as countError from finalErrorCheck")
    // Error condition check
    val error: Long = finalErrorCheck.collect()(0).getLong(0)
    if (error > 1) {
      throw new Exception("More than one odd value pairs")
    } else if (error == 0) {
      throw new Exception("No odd value pairs")
    }
    // write to output with tab seperated format
    resultTableDf.write.mode(SaveMode.Overwrite).option("delimiter", "\t").csv(outputPath)
    logger.info("Finished  Spark")
    spark.stop()
  }


  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }

}
