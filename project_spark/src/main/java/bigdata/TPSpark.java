package bigdata;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;


import static bigdata.Const.*;
import static bigdata.HeightOperations.toColorArray;
import static bigdata.HeightOperations.toShortArray;
import static bigdata.ImageOperations.*;

public class TPSpark {

	public static void main(String[] args) throws Exception{

		SparkConf conf = new SparkConf().setAppName("Projet BIGdata");
		JavaSparkContext context = new JavaSparkContext(conf);

		String pathLouis = "hdfs://young:9000/user/lleduc/hgt/";

		JavaPairRDD<String, PortableDataStream> mainRDD = context.binaryFiles(pathLouis);

		JavaPairRDD<String, short[]> shortRDD = toShortArray(mainRDD);
		JavaPairRDD<String, int[]> colorRDD = toColorArray(shortRDD);
		colorRDD = getPosAbs(colorRDD);

		generateDefaultImage();

		// Generates the original tiles (at zoom 5) and the sub tiles (until zoom 10)
		/*for(int z=5; z<NB_ZOOM; z++) {
			imageOperations.getSubImages(colorRDD, z);
		}*/

		// Generates zoom 3 and 4 by grouping 4 images
		for(int i=0; i<2; i++){
			JavaPairRDD<String, int[][]> unzoomedRDD = groupInSquare(colorRDD, 2);
			colorRDD = applyMeanAndMergeImages(unzoomedRDD, 4-i);
		}

		// Generates zoom 1 and 2 by grouping 9 images
		for(int i=0; i<2; i++){
			JavaPairRDD<String, int[][]> unzoomedRDD = groupInSquare(colorRDD, 3);
			colorRDD = applyMeanAndMergeImages(unzoomedRDD, 2-i);
		}

		// Generates the original image with the full map
		JavaPairRDD<String, int[][]> unzoomedRDD = groupInSquare(colorRDD, 2);
		colorRDD = applyMeanAndMergeImages(unzoomedRDD, 0);

		// Factor 5
        colorRDD.count();
	}
}
