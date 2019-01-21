package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TPSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Projet BIGdata");
		JavaSparkContext context = new JavaSparkContext(conf);
		context.sequenceFile("user/pascal/dem3seq");

	}
	
}
