package spark.tests;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
//Example of spark application to find soccer players of a certain position and country in a fifa data set
public class PlayersByCountrySpark {

	public static void main(String[] args) {
		
		if (args.length != 4) {
			System.err.println("Please inform the input and output paths and the player position/country wanted");
			System.exit(-1);
		}
		SparkConf sparkConf = new SparkConf();//Used to provide useful properties to the application
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);//To communicate with the file system
		JavaRDD<String> dataset = sparkContext.textFile(args[0]);//Loading file with the path provided on execution
		JavaRDD<String[]> records = dataset.map(new Function<String, String[]>() {
			public String[] call(String rawData) throws Exception {
				return rawData.split(",");//If Java 8 runtime is available we can use lambda expressions instead of anonymous class 
			}
		});
		final String position = args[1];
		final String country = args[2];
		JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {
			public Boolean call(String[] records) throws Exception {
				return records[21] == position && records[5] == country;
			}
		});
		JavaPairRDD<String, String> keyValue  = filtered.mapToPair(new PairFunction<String[], String, String>() {
			public Tuple2<String, String> call(String[] filtered) throws Exception {
				return new Tuple2<String, String>(filtered[5], filtered[2]);
			}
		});
		JavaPairRDD<String, String> grouped = keyValue.reduceByKey(new Function2<String, String, String>() {
			public String call(String key, String value) throws Exception {
				return key;
			}
		});
		grouped.saveAsTextFile(args[3]);
	}
	
}
