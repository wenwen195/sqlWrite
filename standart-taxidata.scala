bin/spark-shell --packages com.databricks:spark-csv_2.10:1.2.0,org.joda:joda-convert:1.2 --jars htrace-core-3.1.0-incubating.jar,guava-12.0.1.jar --num-executors 6  --driver-memory 15G --executor-memory 32G --executor-cores 20



import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD 
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, IllegalFieldValueException}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._ 
import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapred.JobConf

class Preprocess() extends java.io.Serializable{

def transferData(input: String, output: String): Unit={
	val rawData = sc.textFile(input)
	//原文件分隔符为“|”
	val rdd = rawData.map(_.split("\\|"))	
	rdd.map((tuple) => tuple.toBuffer.remove(1,7). //原文件的第1,7列(从0数起)为无用列，移除
					   toArray.mkString(",")).//将分隔符改为“,”
					   saveAsTextFile(output)//将原文件输出到新文件
	}
}

val input = "/import-data/16data/taxi201603/20160321.txt"//读入文件
val output = "/import-data/16data/taxi201603/20160321.csv"//输出文件
val pre = new Preprocess()
pre.transferData(input, output)


