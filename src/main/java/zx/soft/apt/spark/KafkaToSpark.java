package zx.soft.apt.spark;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import zx.soft.apt.spark.TestTreeSet.PcapSub;
import zx.soft.apt.utis.ConvertUtil;

public class KafkaToSpark {

	public static void main(String[] args) {

		String brokers = "kafka01:9092,kafka02:9092,kafka03:9092";
		String topics = "apt-cache";
		int duration = Integer.valueOf(args[1]);

		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster(args[0]);
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(duration));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(jssc, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, topicsSet);

		//获取kafka里面的数据
		JavaDStream<byte[]> messageDStream = messages.map(new Function<Tuple2<String, byte[]>, byte[]>() {
			@Override
			public byte[] call(Tuple2<String, byte[]> tuple2) {
				return tuple2._2();
			}
		});

		//对每一条message<byte[]>处理，转换成<String,byte[]>
		JavaPairDStream<String, byte[]> messagePairDStream = messageDStream
				.mapToPair(new PairFunction<byte[], String, byte[]>() {
					@Override
					public Tuple2<String, byte[]> call(byte[] message) {
						return translate(message);
					}

				});

		//通过keygroup，转换成<String,Iterable<byte[]>>
		JavaPairDStream<String, Iterable<byte[]>> keyLists = messagePairDStream.groupByKey();

		//将函数作用于每个key对应的values
		JavaPairDStream<String, Integer> keysize = keyLists.mapValues(new Function<Iterable<byte[]>, Integer>() {
			@Override
			public Integer call(Iterable<byte[]> v1) throws Exception {
				//	combine(v1);
				return combineValue(v1);
			}

		});

		keysize.print();

		jssc.start();
		jssc.awaitTermination();
	}

	private static Tuple2<String, byte[]> translate(byte[] message) {
		byte[] value = new byte[message.length - 24];
		byte[] ip = new byte[16];
		byte[] ipv4 = new byte[4];

		ByteBuffer buffer = ByteBuffer.wrap(message);
		buffer.get(ip, 0, 16);
		buffer.order(ByteOrder.BIG_ENDIAN);
		long timestamp = buffer.getLong();

		StringBuilder b = new StringBuilder();
		//IPv4,时间戳,块标识信息
		if (Byte.valueOf(ip[0]) != null && Byte.valueOf(ip[0]) == 0) {
			System.arraycopy(ip, 12, ipv4, 0, 4);
			try {
				b.append(String.valueOf(timestamp))
						.append("|")
						.append(InetAddress.getByAddress(ipv4).toString()
								.substring(1, InetAddress.getByAddress(ipv4).toString().length()));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		} else {
			try {
				b.append(String.valueOf(timestamp))
						.append("|")
						.append(InetAddress.getByAddress(ip).toString()
								.substring(1, InetAddress.getByAddress(ip).toString().length()));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		System.arraycopy(message, 24, value, 0, message.length - 24);
		return new Tuple2<String, byte[]>(b.toString(), value);
	}

	private static int combineValue(Iterable<byte[]> v1) {
		int i = 0;
		Iterator<byte[]> iterator = v1.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			i++;
		}
		return i;
	}

	private static void combine(Iterable<byte[]> v1) throws IOException {
		Iterator<byte[]> iterator = v1.iterator();
		TreeSet<PcapSub> treeSet = new TreeSet<>();
		while (iterator.hasNext()) {
			byte[] temp = iterator.next();
			byte[] o = new byte[4];
			byte[] data = new byte[temp.length - 4];
			System.arraycopy(temp, 0, o, 0, 4);
			System.arraycopy(temp, 4, data, 0, data.length);
			int order = ConvertUtil.fromByteArray(o);
			PcapSub pcapSub = new PcapSub(order, data);
			treeSet.add(pcapSub);
		}
		Iterator<PcapSub> pcapSubIterator = treeSet.iterator();
		while (pcapSubIterator.hasNext()) {
			PcapSub pcapSub = pcapSubIterator.next();
			if (pcapSub.getOrder() == -1) {
				int length = ConvertUtil.fromByteArray(pcapSub.getValue());
				System.err.println("expext:" + length + "actural:" + treeSet.size());
			}
		}
	}
}
