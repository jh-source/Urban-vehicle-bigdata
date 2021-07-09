package j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Author: cwz
 * Time: 2017/9/20
 * Description: 获取本地或者集群SparkContext
 */
public class SC {

    /**
     * 本地开发模式，不需要连接spark集群
     * local[2] 表示在本机以两线程运行
     * @param appName 自定义应用名字
     * @return
     */
    public static JavaSparkContext getLocalSC(String appName) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(appName);
        return new JavaSparkContext(conf);
    }

    /**
     * 集群开发模式，程序提交到spark集群进行运行，setJars设定此项目打包后的位置，打包中依赖不需要spark-assembly-1.4.0-hadoop2.6.0.jar
     *  ebd-m 为spark master所在的虚拟机主机名，请修改为自己集群中对应主机名
     * @param appName 自定义应用名字
     * @return
     */
    public static JavaSparkContext getCloudSC(String appName) {
        SparkConf conf = new SparkConf()
                .setMaster("spark://192.168.1.10:7077")
                .setAppName(appName)
                .setJars(new String[]{"out\\artifacts\\sparkhbase_jar\\sparkhbase.jar"});
        return new JavaSparkContext(conf);
    }
}
