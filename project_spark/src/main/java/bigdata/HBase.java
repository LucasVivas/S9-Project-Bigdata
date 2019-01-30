package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import java.io.IOException;

public class HBase extends Configured implements Tool {

    private static Connection connection;
    private static Table table;

    public static final byte[] TABLE_NAME = Bytes.toBytes("acfranger_lvivas");
    public static final byte[] TILE = Bytes.toBytes("position");

    public static final byte[] X = Bytes.toBytes("x");
    public static final byte[] Y = Bytes.toBytes("y");
    public static final byte[] ZOOM = Bytes.toBytes("zoom");
    public static final byte[] IMG = Bytes.toBytes("img");

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
            HColumnDescriptor position = new HColumnDescriptor(TILE);
            descriptor.addFamily(position);
            create(admin, descriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static Put createPut(byte[] img, int x, int y, int z){
        String Zoom = "Z" + z;
        String XPos = "X" + x;
        String YPos = "Y" + y;
        String row = Zoom + XPos + YPos;
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(TILE, X, Bytes.toBytes(XPos));
        put.addColumn(TILE, Y, Bytes.toBytes(YPos));
        put.addColumn(TILE, ZOOM, Bytes.toBytes(Zoom));
        put.addColumn(TILE, IMG, img);
        return put;
    }

    public static void createAndPutRow(byte[] img, int x, int y, int z){
        Put put = createPut(img, x, y, z);
        try {
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private static Put createDefaultPut(byte[] img){
        String row = "default";
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(TILE, IMG, img);
        return put;
    }

    public static void createDefaultRow(byte[] img){
        Put put = createDefaultPut(img);
        try {
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws IOException{
        Configuration conf = getConf();
        connection = ConnectionFactory.createConnection(conf);
        createTable(connection);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
        return 0;
    }
}