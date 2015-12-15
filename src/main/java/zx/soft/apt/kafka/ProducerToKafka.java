package zx.soft.apt.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import zx.soft.utils.config.ConfigUtil;

public class ProducerToKafka {

	public static void main(String[] args) {

		KafkaProducer kafkaProducer = new KafkaProducer(createProps());
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", "just for test");
		kafkaProducer.send(producerRecord);
		kafkaProducer.close();
	}

	public static Properties createProps() {

		Properties kafka = ConfigUtil.getProps("kafka.properties");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getProperty("bootstrap.servers"));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.TIMEOUT_CONFIG, kafka.getProperty("timeout.ms", "50000"));
		props.put(ProducerConfig.ACKS_CONFIG, kafka.getProperty("acks", "1"));
		return props;
	}

}
