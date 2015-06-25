import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * Created by imu on 6/25/2015.
 */
public class Consumer extends Thread
{
	private final ConsumerConnector consumer;
	private final String topic;

	public Consumer(String topic)
	{
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
				createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig()
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", "128.199.82.219:2181");
		props.put("group.id", "group1");
		props.put("zookeeper.session.timeout.ms", "2000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);

	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, 1);
		StringDecoder decoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, decoder, decoder);
		KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
		for (MessageAndMetadata<String, String> messageAndMetadata : stream) {
			System.out.println(messageAndMetadata.message());
		}
	}

	public static void main(String[] args)
	{
		Consumer consumerThread = new Consumer("test");
		consumerThread.start();

	}
}
