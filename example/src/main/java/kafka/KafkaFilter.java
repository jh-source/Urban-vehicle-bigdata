package kafka;

import com.google.gson.Gson;
import data.Record;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import redis.Publisher;
import redis.SubThread;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaFilter
{
    private static String topicName = "fifth";
    private static String kafkaClusterIP = "172.23.84.244:9092，172.20.37.106:9092";
    private static Record record;
    private static Gson gson = new Gson();

    private static String key_wrong = "wrong"; //the key of wrong data
    private static String redisHost = "172.22.240.63";
    private static int redisPort = 6379;
    private static Jedis jedis = new Jedis(redisHost, redisPort, 10000);

    private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "172.22.240.63", 6379);
    //private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.1.10", 6379);

    static void kafkaToRedis()
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterIP);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);


        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> stream_1 = builder.stream(topicName);

        System.out.println("handling...");

        stream_1.filter((key, value) ->{

                record = gson.fromJson(value, Record.class);

                if(record.getLongitude()>130 || record.getLatitude()>40)
                    {
                    jedis.sadd(key_wrong,value);
                    return false;
                }
                else
                {
                    jedis.publish("mychannel", value);
//                    System.out.println(value);
                    return true;
                }
            }
    ).to("after");
        System.out.println("ok...");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

    public static void main(String[] args)
    {

        System.out.println(String.format("redis pool is starting, redis ip %s, redis port %d", "192.168.1.10", 6379));
        SubThread subThread = new SubThread(jedisPool);  //订阅者
        subThread.start();

        Publisher publisher = new Publisher(jedisPool);    //发布者
        publisher.start();
        kafkaToRedis();

    }
}
