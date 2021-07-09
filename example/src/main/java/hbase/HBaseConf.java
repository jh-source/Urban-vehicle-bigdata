package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;


/**
 *  类描述：
 *      HBase配置类，负责做HBase连接的工作，对数据库操作提供Table连接对象
 */

public class HBaseConf {
    private static Configuration configuration;
    private static Connection connection;

    static{
        configuration = new Configuration();
        //这里是HBase连接配置，只需要改一下主机名即可，不需要改变端口
        //configuration.set("hbase.zookeeper.quorum","ljh0:2181,ljh1:2181,ljh2:2181");
        configuration.set("hbase.zookeeper.quorum","hbase1:2181,hbase2:2181,Spark:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            System.out.println("初始化连接HBase数据库失败");
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    public static Table getTableByName(String tableName){
        if(connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                System.out.println("HBase数据库连接关闭，尝试重新连接失败");
                e.printStackTrace();
            }
        }

        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            System.out.println("连接数据库表失败");
            e.printStackTrace();
        }
        return null;
    }
}
