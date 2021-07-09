package redis;
import com.google.gson.Gson;
import data.Record;
import hbase.HBaseInsert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Subscriber extends JedisPubSub {

    private static HBaseInsert in = new HBaseInsert();
    private static int i = 0;

    public Subscriber(){}
    @Override
    public void onMessage(String channel, String message) {       //收到消息会调用
        //System.out.println(String.format("receive redis published message, channel %s, message %s", channel, message));
        List list = new ArrayList();
        Record record;
        Gson gson = new Gson();
        record = gson.fromJson(message,Record.class);
        list.add(record);
        in.insertRecordsToHBase(list);
        i = i + 1;
        System.out.println("suceessful!" + i);
    }
    @Override
    public void onSubscribe(String channel, int subscribedChannels) {    //订阅了频道会调用
        System.out.println(String.format("subscribe redis channel success, channel %s, subscribedChannels %d",
                channel, subscribedChannels));
    }
    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {   //取消订阅 会调用
        System.out.println(String.format("unsubscribe redis channel, channel %s, subscribedChannels %d",
                channel, subscribedChannels));

    }
}
