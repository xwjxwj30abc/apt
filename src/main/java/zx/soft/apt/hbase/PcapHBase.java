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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.config.ConfigUtil;

public class PcapHBase {

	private static Logger logger = LoggerFactory.getLogger(PcapHBase.class);
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

	//	public void put(ConsumerRecord<String, byte[]> record) throws UnknownHostException {
	//
	//		ByteBuffer buffer = ByteBuffer.wrap(record.value());
	//		byte[] value = new byte[record.value().length - 28];
	//		byte[] ip = new byte[16];
	//		byte[] ipv4 = new byte[4];
	//		buffer.get(ip, 0, 16);
	//		buffer.order(ByteOrder.BIG_ENDIAN);
	//		long timestamp = buffer.getLong();
	//		int order = buffer.getInt();
	//
	//		if (Byte.valueOf(ip[0]) != null && Byte.valueOf(ip[0]) == 0) {
	//			System.arraycopy(ip, 12, ipv4, 0, 4);
	//			//			logger.info("ip=" + InetAddress.getByAddress(ipv4) + ";timestamp=" + timestamp + ";order=" + order
	//			//					+ ";value.size=" + value.length);
	//		} else {
	//			//			logger.info("ip=" + InetAddress.getByAddress(ip) + ";timestamp=" + timestamp + ";order=" + order
	//			//					+ ";value.size=" + value.length);
	//		}
	//
	//		StringBuilder b = new StringBuilder();
	//		b.append(
	//				InetAddress.getByAddress(ipv4).toString()
	//				.substring(1, InetAddress.getByAddress(ipv4).toString().length())).append("|")
	//				.append(String.valueOf(timestamp));
	//		Put put = new Put(Bytes.toBytes(b.toString()));
	//
	//		if (order == -1) {
	//			//logger.info("ip=" + InetAddress.getByAddress(ip) + ";ts=" + timestamp + ";file size=" + size);
	//			int size = buffer.getInt();
	//			put.add(Bytes.toBytes("apt-cache"), Bytes.toBytes(String.valueOf(65535)),
	//					Bytes.toBytes(String.valueOf(size)));
	//		} else {
	//			//buffer.get(value, 0, value.length);
	//			value = convertToContent(record.value());
	//			put.add(Bytes.toBytes("apt-cache"), Bytes.toBytes(String.valueOf(order)), value);
	//		}
	//		puts.add(put);
	//	}

	//	public void execute() {
	//		try {
	//			table.put(puts);
	//			logger.info("put to HBase:" + puts.size());
	//			puts.clear();
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}
	//	}

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
