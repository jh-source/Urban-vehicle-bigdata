package s

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: cwz
  * Time: 2017/9/19
  * Description: 获取本地或者集群SparkContext
  */
object SC {

  /**
    * 本地开发模式，不需要连接spark集群
    * local[2] 表示在本机以两线程运行
    * @param appName 自定义应用名字
    * @return
    */
  def getLocalSC(appName: String): SparkContext = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(appName)
    new SparkContext(conf)
  }

  /**
    * 集群开发模式，程序提交到spark集群进行运行，setJars设定此项目打包后的位置，打包中依赖不需要spark-assembly-1.4.0-hadoop2.6.0.jar
    * ebd-m 为spark master所在的虚拟机主机名，请修改为自己集群中对应主机名
    * @param appName 自定义应用名字
    * @return
    */
  def getCloudSC(appName: String): SparkContext = {
    val conf: SparkConf = new SparkConf()
      .setMaster("spark://ebd-m:7077")
      .setAppName(appName)
      .setJars(Array("out\\artifacts\\sparkhbase_jar\\sparkhbase.jar"))
    new SparkContext(conf)
  }
}
