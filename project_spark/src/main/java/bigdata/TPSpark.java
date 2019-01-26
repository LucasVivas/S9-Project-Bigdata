package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TPSpark {
    private static final int SIZE_TUILE_X = 1201;
    private static final int SIZE_TUILE_Y = 1201;
    public static final int NB_TUILE_X = 1;
    public static final int NB_TUILE_Y = 1;
    public static final int SIZE_SUBTUILE_X = 1200/NB_TUILE_X;
    public static final int SIZE_SUBTUILE_Y = 1200/NB_TUILE_Y;
    private static final Color [] colorScale = {
            new Color(0,0,255),
            new Color(0,60,48),
            new Color(1,102,94),
            new Color(53,151,143),
            new Color(128,205,193),
            new Color(199,234,229),
            new Color(246,232,195),
            new Color(223,194,125),
            new Color(191,129,45),
            new Color(140,81,10),
            new Color(84,48,5)
    };
    public static final int [] heightScale = {0, 5, 10, 25, 50, 75, 100, 150, 200, 225, 255};

    static String splitName(String path){
        String [] pathSplited = path.split("/");
        String [] nameSplited = pathSplited[6].split("\\.");
        return nameSplited[0];
    }

    static JavaPairRDD<String, short[]> toShortArray(JavaPairRDD<String, PortableDataStream> rdd){
        JavaPairRDD<String, short[]>newRDD = rdd.mapToPair(tuileTuple -> {
            String name = splitName(tuileTuple._1);
            PortableDataStream pds = tuileTuple._2;
            byte[] contentByteArray = pds.toArray();
            short[] outputShortArray = new short[SIZE_TUILE_X * SIZE_TUILE_Y];
            ByteBuffer.wrap(contentByteArray).order(ByteOrder.BIG_ENDIAN).asShortBuffer().get(outputShortArray);
            return new Tuple2<>(name, outputShortArray);
        });
        return newRDD;
    }

    static int toColor(short s){
        Color color = colorScale[0];
        if (s>255)
            color = colorScale[10];
        else {
            for (int i = 0; i < heightScale.length - 1; i++) {
                if (s > heightScale[i] && s <= heightScale[i + 1]) {
                    color = colorScale[s / 25];
                }
            }
        }
        return color.getRGB();
    }

    static int[] shortToColorArray(short[] s){
      int[] colorArray = new int[SIZE_TUILE_X * SIZE_TUILE_Y];
      for (int i = 0; i < s.length; i++) {
          short x = s[i];
          colorArray[i] = toColor(x);
      }
      return colorArray;
    }

    static JavaPairRDD<String, int[]> toColorArray(JavaPairRDD<String, short[]> rdd){
        JavaPairRDD<String, int[]> newRDD = rdd.mapToPair(tuileTuple -> {
            String name = tuileTuple._1;
            short[] shortArray = tuileTuple._2;
            int[] colorArray = shortToColorArray(shortArray);
            return new Tuple2<>(name, colorArray);
        });
        return newRDD;
    }

		public static void getSubImages(JavaPairRDD<String, int[]> colorRDD){
            colorRDD.foreach(colorTuile -> {
                        String name = colorTuile._1;
                        int [] colors = colorTuile._2;
                        System.out.println("---------------------");
                        System.out.println(name);

                        BufferedImage image = new BufferedImage(SIZE_TUILE_X, SIZE_TUILE_Y, BufferedImage.TYPE_INT_RGB);
                        image.setRGB(0, 0, SIZE_TUILE_X, SIZE_TUILE_Y, colors, 0, SIZE_TUILE_X);
                        for (int y = 0; y < NB_TUILE_X; y++) {
                            for (int x = 0; x < NB_TUILE_Y; x++) {
                                ImageIO.write(image.getSubimage(SIZE_SUBTUILE_X*x, SIZE_SUBTUILE_Y*y,
                                        SIZE_SUBTUILE_X, SIZE_SUBTUILE_Y), "png", new File
                                        ("output/output"+name +"X" + y +"Y"+
                                        x + ".png"));
                            }
                        }
                    }
            );
		}

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
