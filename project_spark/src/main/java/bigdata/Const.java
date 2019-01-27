package bigdata;

import java.awt.*;

public class Const {
    public static final int SIZE_TUILE_X = 1201;
    public static final int SIZE_TUILE_Y = 1201;
    public static final int SIZE_GRID_X = 360;
    public static final int SIZE_GRID_Y = 180;
    public static final int NB_TUILE_X = 1;
    public static final int NB_TUILE_Y = 1;
    public static final int SIZE_SUBTUILE_X = 1200/NB_TUILE_X;
    public static final int SIZE_SUBTUILE_Y = 1200/NB_TUILE_Y;
    public static final Color[] colorScale = {
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
}
