package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

public class TPSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Projet BIGdata");
		JavaSparkContext context = new JavaSparkContext(conf);

		String path00 = "hgt/N43W001.hgt";
		String path01 = "hgt/N43W002.hgt";
		String path10 = "hgt/N44W001.hgt";
		String path11 = "hgt/N44W002.hgt";

		JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles
				("hdfs://young:9000/user/pascal/dem3seq/N43W001.hgt");


	}
	
}
