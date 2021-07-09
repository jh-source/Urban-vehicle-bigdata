package j;

import org.apache.hadoop.conf.Configuration;

/**
 * Author: cwz
 * Time: 2017/9/20
 * Description: 设置hbase连接配置
 */
public class HbaseConf {

    // 配置hbase的主节点ip

    private final static String master = "192.168.1.10";
    // 配置hbase的zookeeper节点ip
    private final static String zookeeper = "192.168.1.10,192.168.1.11,192.168.1.12";
    // 配置hbase的zookeeper连接端口，默认值为2181
    private final static String port = "2181";

    public static Configuration getConf() {
        Configuration hbaseConf = new Configuration();
        hbaseConf.set("hbase.master", master);
        hbaseConf.set("hbase.zookeeper.quorum", zookeeper);
        hbaseConf.set("hbase.zookeeper.property.clientPort", port);
        return hbaseConf;
    }

}
