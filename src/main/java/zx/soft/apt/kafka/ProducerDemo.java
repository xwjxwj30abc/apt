package zx.soft.apt.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka生产者示例
 * @author fgq
 *
 */
public class ProducerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
		//		props.put("client.id", "ProducerDemo");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 21960);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// Producer
		Producer<String, String> producer = new KafkaProducer<>(props);
		/* message */
		Boolean isAsync = Boolean.TRUE; // 是否异步
		for (int i = 0; i < 10_000_000; i++) {
			if (isAsync) { // 发送异步消息
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				producer.send(
						new ProducerRecord<String, String>("apt-receive1", Integer.toString(i), Integer.toString(i)),
						new DemoCallBack(System.currentTimeMillis(), i, Integer.toString(i)));
			} else { // 发送同步消息
				try {
					producer.send(
							new ProducerRecord<String, String>("apt-receive1", Integer.toString(i), Integer.toString(i)))
							.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		//		producer.flush();
		/* close pool */
		producer.close();

		System.out.println("Finish!");
	}

	private static class DemoCallBack implements Callback {

		private long startTime;
		private int key;
		private String message;

		public DemoCallBack(long startTime, int key, String message) {
			this.startTime = startTime;
			this.key = key;
			this.message = message;
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			if (metadata != null) {
				System.out.println("key: " + key);
				//				System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition()
				//						+ "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
			} else {
				exception.printStackTrace();
			}
		}

	}

}