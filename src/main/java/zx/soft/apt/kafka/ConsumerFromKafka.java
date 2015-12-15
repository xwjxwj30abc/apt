package zx.soft.apt.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.config.ConfigUtil;

public class ConsumerFromKafka {

	private static Logger logger = LoggerFactory.getLogger(ConsumerFromKafka.class);

	public static void main(String[] args) {
		AtomicLong count = new AtomicLong(0);
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig());
		List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerConnector
				.createMessageStreamsByFilter(new Whitelist("test"));
		for (final KafkaStream<byte[], byte[]> stream : kafkaStreamList) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				count.incrementAndGet();
				logger.info(it.next().message().toString());
			}
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties kafka = ConfigUtil.getProps("kafka.properties");
		Properties props = new Properties();
		props.put("zookeeper.connect", kafka.getProperty("zookeeper"));
		props.put("group.id", kafka.getProperty("group_id"));
		props.put("zookeeper.session.timeout.ms", "60000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}
}
