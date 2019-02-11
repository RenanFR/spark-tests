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
		final String position = args[2];
		final String country = args[3];
		String inputPath = args[0];
		String outputPath = args[1];
		System.out.println("Selected position " + position + " and country " + country);
		System.out.println("Input path at " + inputPath + " and output path at " + outputPath);
		SparkConf sparkConf = new SparkConf();//Used to provide useful properties to the application
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);//To communicate with the file system
		JavaRDD<String> dataset = sparkContext.textFile(inputPath);//Loading file with the path provided on execution
		JavaRDD<String[]> records = dataset.map(new Function<String, String[]>() {
			public String[] call(String rawData) throws Exception {
				return rawData.split(";");//If Java 8 runtime is available we can use lambda expressions instead of anonymous class 
			}
		});
		System.out.println("Number of records: " + records.count());
		JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {
			public Boolean call(String[] records) throws Exception {
				if (records.length != 18) return false;
				return records[12].matches(position) && records[2].matches(country);
			}
		});
		System.out.println("Founded players by criteria " + filtered.count());
		JavaPairRDD<String, String> keyValue  = filtered.mapToPair(new PairFunction<String[], String, String>() {
			public Tuple2<String, String> call(String[] filtered) throws Exception {
				return new Tuple2<String, String>(filtered[2], filtered[0]);
			}
		});
		JavaPairRDD<String, Iterable<String>> grouped = keyValue.groupByKey();
		System.out.println("Result count after grouping " + grouped.count());
		grouped.saveAsTextFile(outputPath);
		System.out.println("Result file stored at " + outputPath);
	}
	
}
