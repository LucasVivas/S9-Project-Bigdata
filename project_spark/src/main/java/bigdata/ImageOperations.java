package bigdata;

import com.twitter.chill.Tuple1LongSerializer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Int;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import static bigdata.Const.*;

public class ImageOperations extends Configured implements Serializable {

    public static Point2D.Double getImagePosition(String name) {
        char[] nameArray = name.toCharArray();
        int x = 0;
        int y = 0;
        if (Character.toLowerCase(nameArray[0]) == 'n') {
            y = 90 - (Character.getNumericValue(nameArray[1]) * 10 + Character.getNumericValue(nameArray[2]));
        } else if (Character.toLowerCase(nameArray[0]) == 's') {
            y = 90 + (Character.getNumericValue(nameArray[1]) * 10 + Character.getNumericValue(nameArray[2]));
        }
        if (Character.toLowerCase(nameArray[3]) == 'w') {
            x = 180 - (Character.getNumericValue(nameArray[4]) * 100 + Character.getNumericValue(nameArray[5]) * 10 + Character.getNumericValue(nameArray[6]));
        } else if (Character.toLowerCase(nameArray[3]) == 'e') {
            x = 180 + (Character.getNumericValue(nameArray[4]) * 100 + Character.getNumericValue(nameArray[5]) * 10 + Character.getNumericValue(nameArray[6]));
        }
        Point2D.Double position = new Point2D.Double(x, y);
        return position;
    }

    public static JavaPairRDD<String, int[]> getPosAbs(JavaPairRDD<String, int[]> colorRDD) {
        JavaPairRDD<String, int[]> newRDD = colorRDD.mapToPair(colorTuile -> {
            String name = colorTuile._1;
            Point2D.Double position = getImagePosition(name);
            String newName = (int)(position.getX()) + "." + (int)(position.getY());
            return new Tuple2<>(newName, colorTuile._2);
        });
        return newRDD;
    }

    public static Position splitName(String name) {
        String[] position = name.split("\\.");
        int x = Integer.parseInt(position[0]);
        int y = Integer.parseInt(position[1]);
        return new Position(x, y);
    }

    public static void generateDefaultImage() throws Exception{
        int[] defaultTmage = new int[SIZE_TUILE_X*SIZE_TUILE_Y];
        for(int i=0; i<defaultTmage.length; i++){
            defaultTmage[i] = 0;
        }
        BufferedImage image = new BufferedImage(SIZE_TUILE_X, SIZE_TUILE_Y, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, SIZE_TUILE_X, SIZE_TUILE_Y, defaultTmage, 0, SIZE_TUILE_X);
        ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
        ImageIO.write(image, "png", byteArrayOS);
        HBase.createDefaultRow(byteArrayOS.toByteArray());

    }

    public void getSubImages(JavaPairRDD<String, int[]> colorRDD, int zoom){
        int tileBySide = (int)Math.pow(2,zoom);
        int sizeSubTuile = 1200/tileBySide;
        colorRDD.foreach(colorTuile -> {
                    String name = colorTuile._1;
                    int[] colors = colorTuile._2;

                    Position position = splitName(name);

                    BufferedImage image = new BufferedImage(SIZE_TUILE_X, SIZE_TUILE_Y, BufferedImage.TYPE_INT_RGB);
                    image.setRGB(0, 0, SIZE_TUILE_X, SIZE_TUILE_Y, colors, 0, SIZE_TUILE_X);
                    for (int y = 0; y < tileBySide; y++) {
                        for (int x = 0; x < tileBySide; x++) {
                            ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
                            ImageIO.write(image.getSubimage(sizeSubTuile*x, sizeSubTuile*y, sizeSubTuile, sizeSubTuile), "png", byteArrayOS);
                            int XPos = x+((tileBySide)*position.getX());
                            int YPos = y+((tileBySide)*position.getY());
                            HBase.createAndPutRow(byteArrayOS.toByteArray(),XPos, YPos, zoom);
                        }
                    }
                }
        );
    }

    /* nbSplit = 3
    * 0|1|2
    * -----
    * 3|4|5
    * -----
    * 6|7|8
    * */
    public static JavaPairRDD<String, int[][]> groupInSquare(JavaPairRDD<String, int[]> colorRDD, int nbSplit) {
        JavaPairRDD<String, Tuple2<Position, int[]>> newRDD = colorRDD.mapToPair(img -> {
            String name = img._1;
            Position position = splitName(name);

            int newX = position.getX() / nbSplit;
            int newY = position.getY() / nbSplit;
            String newName = newX + "." + newY;

            Position pos = new Position(position.getX() % nbSplit, position.getY() % nbSplit);
            return new Tuple2<>(newName, new Tuple2<>(pos, img._2));
        });
        JavaPairRDD<String, Iterable<Tuple2<Position, int[]>>> tmpRDD = newRDD.groupByKey();
        return tmpRDD.mapToPair(square -> {
            int [][] array = new int[nbSplit*nbSplit][];
            for (Tuple2<Position, int[]> tup: square._2) {
                Position pos = tup._1;
                array[pos.getX()+pos.getY()*nbSplit] = tup._2;
            }
            return new Tuple2<>(square._1, array);
        });
    }

    

    public static void getMeanImage(int[][] imagesToMerge, int newX, int newY, int zoomLevel) throws Exception {
        int meanImageLength = SIZE_TUILE_X * SIZE_TUILE_Y;
        int[] meanImage = new int[meanImageLength];
        int nbImages = imagesToMerge.length;
        int imgBySide = (int) Math.sqrt(nbImages);

        for (int i = 0; i < nbImages; i++) {
            int[] image = imagesToMerge[i];
            for (int y = 0; y < SIZE_TUILE_Y / imgBySide; y += imgBySide) {
                for (int x = 0; x < SIZE_TUILE_X / imgBySide; x += imgBySide) {
                    int currentY = ((y / imgBySide) * SIZE_TUILE_Y) + (SIZE_TUILE_Y / imgBySide) * SIZE_TUILE_X * (i / imgBySide);
                    int currentX = x / imgBySide + (SIZE_TUILE_X / imgBySide) * (i % imgBySide);
                    for (int sub = 0; sub < nbImages; sub++) {
                        meanImage[currentY + currentX] += image[(y * SIZE_TUILE_Y) + (sub / imgBySide) * SIZE_TUILE_X + x + (sub % imgBySide)];
                    }
                    meanImage[currentY + currentX] /= nbImages;
                }
            }
        }

        BufferedImage image = new BufferedImage(SIZE_TUILE_X, SIZE_TUILE_Y, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, SIZE_TUILE_X, SIZE_TUILE_Y, meanImage, 0, SIZE_TUILE_X);
        ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
        ImageIO.write(image, "png", byteArrayOS);
        String[] args = {String.valueOf(newX), String.valueOf(newY), String.valueOf(zoomLevel), byteArrayOS.toString()};
        ToolRunner.run(HBaseConfiguration.create(), new HBase(), args);
    }

}
