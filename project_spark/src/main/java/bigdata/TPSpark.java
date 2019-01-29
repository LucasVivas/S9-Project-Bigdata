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
import static bigdata.ImageOperations.generateDefaultImage;
import static bigdata.ImageOperations.getMeanImage;
import static bigdata.ImageOperations.getSubImages;

public class TPSpark {

	public static void main(String[] args) throws Exception{

		SparkConf conf = new SparkConf().setAppName("Projet BIGdata");
		JavaSparkContext context = new JavaSparkContext(conf);

		String pathLouis = "hdfs://young:9000/user/lleduc/hgt/";

		JavaPairRDD<String, PortableDataStream> mainRDD = context.binaryFiles(pathLouis);

		JavaPairRDD<String, short[]> shortRDD = toShortArray(mainRDD);
		JavaPairRDD<String, int[]> colorRDD = toColorArray(shortRDD);

		generateDefaultImage();
		for(int z=0; z<NB_SUBZOOM; z++) {
			getSubImages(colorRDD, z);
		}

		/* for(int i=0; i<2; i++){
			// Factor 2
			int[][] imagesToMerge;
			int XPos;
			int YPos;
			int ZoomLevel;
			JavaPairRDD<String, int[]> unzoomedRDD = getMeanImage(imagesToMerge, XPos, YPos, ZoomLevel);
		}

		for(int i=0; i<2; i++){
			// Factor 3
			int[][] imagesToMerge;
			int XPos;
			int YPos;
			int ZoomLevel;
			getMeanImage();
		}

		// Factor 5 */

		colorRDD.count();
		System.exit(0);
	}
}
