package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.awt.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static bigdata.Const.*;
import static bigdata.Const.SIZE_TUILE_Y;

public class HeightOperations {

    static String splitPath(String path){
        String [] pathSplited = path.split("/");
        String [] nameSplited = pathSplited[6].split("\\.");
        return nameSplited[0];
    }

    static JavaPairRDD<String, short[]> toShortArray(JavaPairRDD<String, PortableDataStream> rdd){
        JavaPairRDD<String, short[]>newRDD = rdd.mapToPair(tuileTuple -> {
            String name = splitPath(tuileTuple._1);
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

}
