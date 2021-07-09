package kafka;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class ProducerExample {

	private String topicName;

	public ProducerExample(String name){
		topicName = name;
	}

	public void sendRecords() throws IOException, InterruptedException {
		Properties props = new Properties();
		//kafka clusterIP  服务器地址，broker的地址清单，建议至少填写两个，避免宕机
		props.put("bootstrap.servers", "192.168.1.10:9092,192.168.1.11:9092");
		//判断是否成功发送
		// acks指定必须有多少个分区副本接收消息，生产者才认为消息写入成功，用户检测数据丢失的可能性
		//acks=0：生产者在成功写入消息之前不会等待任何来自服务器的响应。无法监控数据是否发送成功，但可以以网络能够支持的最大速度发送消息，达到很高的吞吐量。
		//acks=1：只要集群的首领节点收到消息，生产者就会收到来自服务器的成功响应。
		//acks=all：只有所有参与复制的节点全部收到消息时，生产者才会收到来自服务器的成功响应。这种模式是最安全的
		props.put("acks", "1");
		//key和value的序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		BufferedReader br =  new BufferedReader(new FileReader("C:\\Users\\hbvb9\\Desktop\\代码示例\\record.json"));//record file path	读取json文件
		Producer<String, String> producer = new KafkaProducer<>(props);
		int i = 0;//message key
		String record;
		//send record to kafka
		while((record = br.readLine())!=null) {

			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), record), new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null)
						e.printStackTrace();
					System.out.println("The offset of the record we just sent is: " + metadata.offset());
				}
			});
			i++;
		}
		producer.close();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		new ProducerExample("topic699").sendRecords();
	}

}
