package bigdata;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.imageio.ImageIO;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;

import static bigdata.Const.*;

public class ImageOperations {

    public static Point2D.Double getImagePosition(String name){
        char[] nameArray = name.toCharArray();
        int x = 0;
        int y = 0;
        if(nameArray[0] == 'n' || nameArray[0] == 'N'){
            y = 90 - (Character.getNumericValue(nameArray[1])*10 + Character.getNumericValue(nameArray[2]));
        } else if(nameArray[0] == 's' || nameArray[0] == 'S'){
            y = 90 + (Character.getNumericValue(nameArray[1])*10 + Character.getNumericValue(nameArray[2]));
        }
        if(nameArray[3] == 'w' || nameArray[3] == 'W'){
            x = 180 - (Character.getNumericValue(nameArray[4])*100 + Character.getNumericValue(nameArray[5])*10 + Character.getNumericValue(nameArray[6]));
        }else if(nameArray[3] == 'e' || nameArray[3] == 'E'){
            x = 180 + (Character.getNumericValue(nameArray[4])*100 + Character.getNumericValue(nameArray[5])*10 + Character.getNumericValue(nameArray[6]));
        }
        Point2D.Double position = new Point2D.Double(x,y);
        return position;
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
        String[] args = {byteArrayOS.toString()};
        ToolRunner.run(HBaseConfiguration.create(), new HBase(), args);

    }

    public static void getSubImages(JavaPairRDD<String, int[]> colorRDD, int zoom){
        int sizeSubTuile = 1200/(int)Math.pow(2,zoom);
        colorRDD.foreach(colorTuile -> {
                    String name = colorTuile._1;
                    Point2D.Double position = getImagePosition(name);
                    int [] colors = colorTuile._2;

                    BufferedImage image = new BufferedImage(SIZE_TUILE_X, SIZE_TUILE_Y, BufferedImage.TYPE_INT_RGB);
                    image.setRGB(0, 0, SIZE_TUILE_X, SIZE_TUILE_Y, colors, 0, SIZE_TUILE_X);
                    for (int y = 0; y < zoom+1; y++) {
                        for (int x = 0; x < zoom+1; x++) {
                            ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
                            ImageIO.write(image.getSubimage(sizeSubTuile*x, sizeSubTuile*y, sizeSubTuile, sizeSubTuile), "png", byteArrayOS);
                            int XPos = (int)(x+((zoom+1)*position.getX()));
                            int YPos = (int)(y+((zoom+1)*position.getY()));
                            String[] args = {String.valueOf(XPos), String.valueOf(YPos), String.valueOf(zoom), byteArrayOS.toString()};
                            ToolRunner.run(HBaseConfiguration.create(), new HBase(), args);
                        }
                    }
                }
        );
    }


    public static void getMeanImage(int[][] imagesToMerge, int newX, int newY, int zoomLevel) throws Exception{
        int meanImageLength = SIZE_TUILE_X*SIZE_TUILE_Y;
        int[] meanImage = new int[meanImageLength];
        int nbImages = imagesToMerge.length;
        int imgBySide = (int)Math.sqrt(nbImages);

        for(int i=0; i<nbImages; i++){
            int [] image = imagesToMerge[i];
            for(int y = 0; y < SIZE_TUILE_Y/imgBySide; y+=imgBySide) {
                for (int x = 0; x < SIZE_TUILE_X/imgBySide; x+=imgBySide) {
                    int currentY = ((y / imgBySide) * SIZE_TUILE_Y) + (SIZE_TUILE_Y / imgBySide) * SIZE_TUILE_X * (i / imgBySide);
                    int currentX = x / imgBySide + (SIZE_TUILE_X / imgBySide) * (i % imgBySide);
                    for(int sub=0; sub<nbImages; sub++){
                        meanImage[currentY+currentX] += image[(y * SIZE_TUILE_Y) + (sub/imgBySide)*SIZE_TUILE_X + x + (sub%imgBySide)];
                    }
                    meanImage[currentY+currentX] /= nbImages;
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
