package redis;



import com.google.gson.Gson;
import data.Record;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;

public class ClientRedis {
    private Jedis jedis;
    String key = "rdb";
    public static void main(String[] args){

    }

    //与redis建立连接
    public void setRelation(){
        String host = "192.168.1.10";
        int port = 6379;
        jedis = new Jedis(host,port);
    }

    //获取数据,用集合set存储
    public void dataInput(String json){

        Pipeline pipelineq = jedis.pipelined();
        pipelineq.sadd(key,json);
    }

    public List jsonToObject(){
        List list = new ArrayList();
        Record record;
        Gson gson = new Gson();
        for(String jsonString : jedis.smembers(key)){
            record = gson.fromJson(jsonString,Record.class);
            list.add(record);
        }
        return list;
    }

}
