package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.io.IOException;

import static bigdata.Const.NB_TUILE_X;
import static bigdata.Const.SIZE_GRID_X;
import static bigdata.Const.SIZE_GRID_Y;

public class HBase extends Configured implements Tool {

    public static final byte[] TABLE_NAME = Bytes.toBytes("acfranger_lvivas");
    public static final byte[][] ZOOM = {Bytes.toBytes("Zoom0"),
                                         Bytes.toBytes("Zoom1"),
                                         Bytes.toBytes("Zoom2"),
                                         Bytes.toBytes("Zoom3"),
                                         Bytes.toBytes("Zoom4"),
                                         Bytes.toBytes("Zoom5"),
                                         Bytes.toBytes("Zoom6"),
                                         Bytes.toBytes("Zoom7"),
                                         Bytes.toBytes("Zoom8"),
                                         Bytes.toBytes("Zoom9")};

    public static void create(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createTable(Connection connect) {
        try {
            final Admin admin = connect.getAdmin();
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            for(int i=0; i<ZOOM.length; i++){
                HColumnDescriptor zoom = new HColumnDescriptor(ZOOM[i]);
                descriptor.addFamily(zoom);
            }
            create(admin, descriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static Put createRow(int YPos, int zoomLevel) {
        String YRow = "Y" + YPos;
        Put put = new Put(Bytes.toBytes(YRow);
        for(int x=0; x<SIZE_GRID_X*zoomLevel; x++){
            String XCol = "X" + x;
            String img = XCol + YRow + ".png"; // Il faudra récupérer la vraie image
            put.addColumn(ZOOM[zoomLevel], Bytes.toBytes(XCol), Bytes.toBytes(img));
        }
        return put;
    }

    public static void createAllRows(Table table, int zoomLevel){)
        for(int y=0; y<SIZE_GRID_Y*zoomLevel; y++){
            try {
                table.put(createRow(y, zoomLevel));
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public static void createAllZooms(Table table){
        for(int z=0; z<ZOOM.length; z++){
            createAllRows(table, z);
        }
    }

    /* public static JavaPairRDD<ImmutableBytesWritable, Put> saveTileToHbase(JavaPairRDD<String, short[]> rdd){

        JavaPairRDD<ImmutableBytesWritable, Put> rddHeightsBytesPng = rdd.mapToPair(stringTuple2 -> {

            String name = stringTuple2._1;
            short[] heights = stringTuple2._2;
            //byte[] heightsInBytes = (byte[])heights;
            int[] image = HeightOperations.shortToColorArray(heights);
            Point2D.Double position = ImageOperations.getImagePosition(name);

            //Put put = createRow(heightsInByte);
            //return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        return rddHeightsBytesPng;
    } */

    @Override
    public int run(String[] args) throws IOException{
        Configuration conf = getConf();
        Connection connection = ConnectionFactory.createConnection(conf);
        createTable(connection);
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        createAllZooms(table);
        return 0;
    }
}
