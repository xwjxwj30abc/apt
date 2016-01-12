package zx.soft.apt.kafka;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * kafka消费者示例
 * @author fgq
 *
 */
public class ConsumerDemo {

	static int[] bloom = new int[10_000_000];

	public static void main(String[] args) throws UnknownHostException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("apt-receive1"));
		List<TopicPartition> partitions = new ArrayList<>();
		//		partitions.add(new TopicPartition("apt-receive1", 2));
		//		partitions.add(new TopicPartition("apt-receive1", 13));
		//		consumer.assign(partitions);
		for (int i = 0; i < 10000; i++) {
			ConsumerRecords<String, byte[]> records = consumer.poll(100);
			System.out.println(i + ": " + records.count());
			for (ConsumerRecord<String, byte[]> record : records) {
				//				System.out.println(record.key());
				bloom[Integer.parseInt(record.key())] = 1;
			}
			//			if (sum == 10000) {
			//				System.out.println("sum=" + sum);
			//				break;
			//			}

		}
		for (int j = 0; j < 10_000_000; j++) {
			if (bloom[j] == 0) {
				System.err.println("" + j);
			}
		}
		consumer.close();
		System.err.println("Finish!");
	}

}
