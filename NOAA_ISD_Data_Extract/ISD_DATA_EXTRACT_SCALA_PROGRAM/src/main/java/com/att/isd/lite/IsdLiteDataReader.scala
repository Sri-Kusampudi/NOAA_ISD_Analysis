package com.att.isd.lite

import org.apache.spark._

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * This program reads gzip files containing ISD Lite Data and converts them to parquet and stores them. 
 * Uses Spark to process and create DataFrame before storing it as parquet files.
 * Each of the file name has the station code embedded and this is extracted as a new column in the data frame.
 * The parquet files are partitioned by column "Year".
 * The ISD-Lite data contain a fixed-width formatted subset of the complete Integrated Surface Data (ISD)
 * for a select number of observational elements. The data are typically stored in a single file corresponding to
 * the ISD data, i.e. one file per station per year.
 * @author sriku
 * @version 1.0 
 */

object IsdLiteDataReader {
   def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        //Initializing Spark Context configuration
        val conf = new SparkConf().set("spark.sql.warehouse.dir", "file:///C:/Scala_IDE_Eclipse/eclipse/Scala_projects/ATT_Interview_Project").setAppName("textfileReader")
        //setting master as the local mode.
        conf.setMaster("local")
        //Creating Spark Context
        val sc = new SparkContext(conf)
        //Creating Sql Context
        val sqlContext = new SQLContext(sc)

        //Setting the Hadoop Home
        System.setProperty("hadoop.home.dir", "C:\\winutils");

        //Setting the Header for the files downloaded containing ISD(Integrated Surface Data).
        val schemaString = "Year,Month,Day,Hour,ATemp,DTemp,SeaPress,WDirection,WSpeed,SkyCond,LiquidPrecOneHour,LiquidPrecSixHour" 
        //defining the fields for DataFrame
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true)) 
        //Schema definition
        val schema = StructType(fields)
        //creating data frame by reading data from gzip files, and assigning schema.
        // Each row is split as columns by extracting the characters as appropriate, as its a fixed length format.
        val isdLiteDF = sqlContext.createDataFrame(sc.textFile("ATT//*.gz").map { x => getRow(x)}, schema)
            
        //For validating
        //isdLiteDF.show()
        //println("Count -" + isdLiteDF.count())   
       
       //val toInt    = udf[Int, String]( _.toInt)
       //Reformatting the DataFrame by adding the File name as the new column for each row. As it contains the station code.
       val featureDf = isdLiteDF
                        .withColumn("FileName",input_file_name())
                        .withColumn("Year", isdLiteDF("Year"))
                        .withColumn("Month", isdLiteDF("Month"))
                        .withColumn("Day", isdLiteDF("Day"))             
                        .withColumn("Hour", isdLiteDF("Hour"))            
                        .withColumn("ATemp", isdLiteDF("ATemp"))             
                        .withColumn("DTemp", isdLiteDF("DTemp"))              
                        .withColumn("SeaPress", isdLiteDF("SeaPress"))
                        .withColumn("WDirection", isdLiteDF("WDirection"))             
                        .withColumn("WSpeed", isdLiteDF("WSpeed"))
                        .withColumn("SkyCond", isdLiteDF("SkyCond"))             
                        .withColumn("LiquidPrecOneHour", isdLiteDF("LiquidPrecOneHour"))
                        .withColumn("LiquidPrecSixHour", isdLiteDF("LiquidPrecSixHour"))
                        .select("FileName","Year","Month","Day","Hour","ATemp","DTemp","SeaPress","WDirection","WSpeed","SkyCond","LiquidPrecOneHour","LiquidPrecSixHour")
       
       //Write the data from the files as Parquet file partitioned by the column Year.
       featureDf.write.partitionBy("Year").parquet("ATT//parquet//isd_lite_data.parquet") 
   
  }
   
   //Since the data files are fixed length, each Row is defined as below -
  def getRow(x : String) : Row={    
    val columnArray = new Array[String](12)
    columnArray(0)=x.substring(0,4)//year
    columnArray(1)=x.substring(5,7) //month
    columnArray(2)=x.substring(8,10)//day
    columnArray(3)=x.substring(11,13)//hour
    columnArray(4)=x.substring(14,19)//Atemp
    columnArray(5)=x.substring(20,25)//Dtemp
    columnArray(6)=x.substring(26,31)//SeaPress
    columnArray(7)=x.substring(32,37)//WDirection
    columnArray(8)=x.substring(38,43)//WSpeed
    columnArray(9)=x.substring(44,49)//SkyCond
    columnArray(10)=x.substring(50,55)//LiquidPrecOneHour
    columnArray(11)=x.substring(56,61)//LiquidPrecSixHour
    Row.fromSeq(columnArray)  
  }
}