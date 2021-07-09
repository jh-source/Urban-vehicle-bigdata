package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class ConsumerExample {

	private  String topicName;

	public ConsumerExample(String topic){
		topicName = topic;
	}

	public  void consume(){
		Properties props = new Properties();
		//Kafka服务器

		props.put("bootstrap.servers", "192.168.1.10:9092,192.168.1.11:9092");//kafka clousterIP
		//消费者群组ID，发布-订阅模式，即如果一个生产者，多个消费者都要消费，那么需要定义自己的群组，同一个群组内的消费者只有一个能消费到消息
		props.put("group.id", "topic699");
		//true，消费者的偏移量将在后台定期提交；false关闭自动提交位移，在消息被完整处理之后再手动提交位移
		props.put("enable.auto.commit", "true");
		//earliest
		//当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
		//latest
		//当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
		//none
		//topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
		props.put("auto.offset.reset", "earliest");
		//key.serializer和value.serializer示例：将用户提供的key和value对象ProducerRecord转换成字节，你可以使用附带的ByteArraySerizlizaer或StringSerializer处理简单的byte和String类型.
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));

		//consume record
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}

	public static void main(String[] args) {
		new ConsumerExample("topic699").consume();
	}
}
