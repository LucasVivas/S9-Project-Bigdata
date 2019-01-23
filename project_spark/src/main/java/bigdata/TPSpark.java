package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

import java.awt.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TPSpark {
    public static final int SIZE_TUILE_X = 1201;
    public static final int SIZE_TUILE_Y = 1201;

    static JavaPairRDD<String, short[]> toShortArray(JavaPairRDD<String, PortableDataStream> rdd){
        JavaPairRDD<String, short[]>newRDD = rdd.mapToPair(tuileTuple -> {
            String name = tuileTuple._1;
            PortableDataStream pds = tuileTuple._2;
            byte[] contentByteArray = pds.toArray();
            short[] outputShortArray = new short[SIZE_TUILE_X * SIZE_TUILE_Y];
            ByteBuffer.wrap(contentByteArray).order(ByteOrder.BIG_ENDIAN).asShortBuffer().get(outputShortArray);
            return new Tuple2<>(name, outputShortArray);
        });
        return newRDD;
    }

    static JavaPairRDD<String, Color[]> toColorArray(JavaPairRDD<String, short[]> rdd){
        rdd.mapValues(shortArray -> {
            short [] newArray = new short[shortArray.length];
            for (int i = 0; i < shortArray.length; i++) {
                short x = shortArray[i];
                if(x<0) {
                    x = 0;
                }
                else if(x>255) {
                    x = 255;
                }
                newArray[i] = x;
            }
            return newArray;
        });

        JavaPairRDD<String, Color[]> newRDD = rdd.mapToPair(tuileTuple -> {
            String name = tuileTuple._1;
            Color[] colorArray = new Color[SIZE_TUILE_X * SIZE_TUILE_Y];

        });
        return newRDD;
    }

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Projet BIGdata");
		JavaSparkContext context = new JavaSparkContext(conf);


		String pathLouis = "hdfs://young:9000/user/lleduc/hgt/";

		JavaPairRDD<String, PortableDataStream> mainRDD = context.binaryFiles
                (pathLouis);

        JavaPairRDD<String, short[]> shortRDD = toShortArray(mainRDD);

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
