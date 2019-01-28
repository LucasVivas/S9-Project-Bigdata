package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;


import static bigdata.Const.*;
import static bigdata.HeightOperations.toColorArray;
import static bigdata.HeightOperations.toShortArray;
import static bigdata.ImageOperations.getSubImages;

public class TPSpark {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Projet BIGdata");
		JavaSparkContext context = new JavaSparkContext(conf);


		String pathLouis = "hdfs://young:9000/user/lleduc/hgt/";

		JavaPairRDD<String, PortableDataStream> mainRDD = context.binaryFiles
                (pathLouis);

        JavaPairRDD<String, short[]> shortRDD = toShortArray(mainRDD);
        JavaPairRDD<String, int[]> colorRDD = toColorArray(shortRDD);
        getSubImages(colorRDD);

        colorRDD.count();
		/*const int sizeX = 1200;
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
    */
	}
}
