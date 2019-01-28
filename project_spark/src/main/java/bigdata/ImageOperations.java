package bigdata;

import org.apache.spark.api.java.JavaPairRDD;

import javax.imageio.ImageIO;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.File;

import static bigdata.Const.*;
import static bigdata.Const.SIZE_SUBTUILE_X;
import static bigdata.Const.SIZE_SUBTUILE_Y;

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

    public static void getSubImages(JavaPairRDD<String, int[]> colorRDD){
        colorRDD.foreach(colorTuile -> {
                    String name = colorTuile._1;
                    Point2D.Double position = getImagePosition(name);
                    int [] colors = colorTuile._2;

                    BufferedImage image = new BufferedImage(SIZE_TUILE_X, SIZE_TUILE_Y, BufferedImage.TYPE_INT_RGB);
                    image.setRGB(0, 0, SIZE_TUILE_X, SIZE_TUILE_Y, colors, 0, SIZE_TUILE_X);
                    for (int y = 0; y < NB_TUILE_X; y++) {
                        for (int x = 0; x < NB_TUILE_Y; x++) {
                            ImageIO.write(image.getSubimage(SIZE_SUBTUILE_X*x, SIZE_SUBTUILE_Y*y,
                                    SIZE_SUBTUILE_X, SIZE_SUBTUILE_Y), "png", new File
                                    ("output/" + "X" + (int)(x+(NB_TUILE_X*position.getX())) + "Y" +
                                            (int)(y+(NB_TUILE_Y*position.getY())) + ".png"));
                        }
                    }
                }
        );
    }

}