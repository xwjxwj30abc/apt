package zx.soft.apt.hbase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaveToHBase {

	private static Logger logger = LoggerFactory.getLogger(SaveToHBase.class);
	private List<Put> puts = new ArrayList<>();

	public void save() throws IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("apt-cache"));

		PcapHBase hbase = new PcapHBase("kafka_db");
		HTableInterface table = hbase.getTable();

		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			ConsumerRecords<String, byte[]> records = consumer.poll(100);
			if (records.count() > 0) {
				for (ConsumerRecord<String, byte[]> record : records) {
					puts.add(recordToPut(record));

				}
				table.put(puts);
				logger.info("put tu hbase size=" + puts.size());
				puts.clear();
			}
		}
		hbase.closeTable();
		hbase.closeConnection();
	}

	//解析record字节信息并转换为HBase中Put
	private Put recordToPut(ConsumerRecord<String, byte[]> record) throws UnknownHostException {

		byte[] value = new byte[record.value().length - 28];
		byte[] ip = new byte[16];
		byte[] ipv4 = new byte[4];

		ByteBuffer buffer = ByteBuffer.wrap(record.value());
		buffer.get(ip, 0, 16);
		buffer.order(ByteOrder.BIG_ENDIAN);
		long timestamp = buffer.getLong();
		int order = buffer.getInt();

		StringBuilder b = new StringBuilder();
		//IPv4,时间戳,块标识信息
		if (Byte.valueOf(ip[0]) != null && Byte.valueOf(ip[0]) == 0) {
			System.arraycopy(ip, 12, ipv4, 0, 4);
			//			logger.info("ip=" + InetAddress.getByAddress(ipv4) + ";timestamp=" + timestamp + ";order=" + order
			//					+ ";value.size=" + value.length);
			b.append(String.valueOf(timestamp))
					.append("|")
					.append(InetAddress.getByAddress(ipv4).toString()
							.substring(1, InetAddress.getByAddress(ipv4).toString().length()));
		} else {
			logger.info("ip=" + InetAddress.getByAddress(ip) + ";timestamp=" + timestamp + ";order=" + order
					+ ";value.size=" + value.length);
			b.append(String.valueOf(timestamp))
					.append("|")
					.append(InetAddress.getByAddress(ip).toString()
							.substring(1, InetAddress.getByAddress(ip).toString().length()));
		}

		Put put = new Put(Bytes.toBytes(b.toString()));

		if (order == -1) {
			put.add(Bytes.toBytes("apt-cache"), Bytes.toBytes(String.valueOf(65535)),
					Bytes.toBytes(String.valueOf(buffer.getInt())));
		} else {
			System.arraycopy(record.value(), 28, value, 0, record.value().length - 28);
			put.add(Bytes.toBytes("apt-cache"), Bytes.toBytes(String.valueOf(order)), value);
		}
		return put;
	}

}
