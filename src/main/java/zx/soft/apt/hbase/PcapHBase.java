package zx.soft.apt.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

import zx.soft.utils.config.ConfigUtil;

public class PcapHBase {

	static Configuration conf = HBaseConfiguration.create();
	static HConnection conn;
	static HBaseAdmin hbaseAdmin;
	static {
		Properties prop = ConfigUtil.getProps("zookeeper.properties");
		conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"));
		try {
			conn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private HTableInterface table;

	public PcapHBase(String tableName) throws IOException {
		hbaseAdmin = new HBaseAdmin(conn);
		if (!(hbaseAdmin.tableExists(tableName))) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			String columnFamily = "apt-cache";
			tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			hbaseAdmin.createTable(tableDescriptor);
		}
		hbaseAdmin.close();
		try {
			table = conn.getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeTable() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeConnection() {
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public HTableInterface getTable() {
		return table;
	}

}
