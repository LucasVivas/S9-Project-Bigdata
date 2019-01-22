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


		const int sizeX = 1200;
		const int sizeY = 1200;
		const int TILEX = 75;
		const int TILEY = 75;
		const int maxNbTileX = sizeX/TILEX;
		const int maxNbTileY = sizeY/TILEY;

		for(int caseY=0; caseY<maxNbTileY; caseY++){
			for(int caseX=0; caseX<maxNbTileX; caseX++){

				for(int y=caseY*TILEY; y<(caseY+1)*TILEY; y++){
					for(int x=caseX*TILEX; x<(caseX+1)*TILEX; x++){

						tab[y*1201 + x]
					}
				}

			}
		}

	}

}
