package hbase.book;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {
	
	@SuppressWarnings({ "static-access", "deprecation", "resource" })
	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
//		config.set("hbase.zookeeper.quorum", "192.168.245.128");
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("test2");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte[] tablename = htd.getName();
		HTableDescriptor [] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}
		
		HTable table = new HTable(config, tablename);
		byte [] row1 = Bytes.toBytes("row1");
		Put p1 = new Put(row1);
		byte [] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("1"), Bytes.toBytes("value1"));
		table.put(p1);
		Get g = new Get(row1);
		Result result = table.get(g);
		System.out.println("get: " + result);
		admin.close();
	}

}
