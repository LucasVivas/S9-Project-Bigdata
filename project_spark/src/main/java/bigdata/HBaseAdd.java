package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

import static bigdata.HBase.TABLE_NAME;
import static bigdata.HBase.createAndPutRow;
import static bigdata.HBase.createDefaultRow;

public class HBaseAdd extends Configured implements Tool {

    @Override
    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        if (args.length == 1) {
            table.put(createDefaultRow(Bytes.toBytes(args[0])));
        } else {
            int x = Integer.parseInt(args[0]);
            int y = Integer.parseInt(args[1]);
            int z = Integer.parseInt(args[2]);
            byte[] byteArray = Bytes.toBytes(args[3]);
            table.put(createAndPutRow(byteArray, x, y, z));
        }
        return 0;
    }
}
