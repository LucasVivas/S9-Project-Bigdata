package ub.m2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class TileDB extends Configured implements Tool {

    private static Table table;
    private Connection connection;
    public static final byte[] TABLE_NAME = Bytes.toBytes("acfranger_lvivas");

    public byte[] getTile(int z, int x, int y){
        byte[] img = null;
        Get get = new Get(Bytes.toBytes("Z"+z+"X"+x+"Y"+y));//x+"."+y+"."+z));
        try {
            img = table.get(get).value();
        } catch (IOException e){
            e.printStackTrace();
        }
        return img;
    }

    public TileDB() throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.5.25");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    @Override
    public int run(String[] args) throws IOException {
        return 0;
    }
}
