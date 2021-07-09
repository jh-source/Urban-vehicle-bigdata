# xdu-cloudcourse-spark

2017

西电云计算课程大作业Spark数据处理代码示例及简易文档教程。

# 部署运行此项目

本项目包j中为java实现版本，s为scala实现版本，选择一种语言实现即可。对操作系统没有要求，只需确保和集群处于同一网络中。

简单概述部署运行此项目的环境和方法，以

- IntelliJ IDEA-2017.2
- JDK-1.8
- Maven-3.5.0
- Scala-2.10.6

为例，其他版本/工具/类库请自行查找教程示例。

## 准备环境

- 安装[JDK-1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)，并[设置环境变量](http://jingyan.baidu.com/article/925f8cb836b26ac0dde0569e.html)
- 安装[Scala-2.10.6](http://www.scala-lang.org/download/2.10.6.html),并设置[环境变量](http://www.runoob.com/scala/scala-install.html)
- 安装IntelliJ IDEA
- 下载此项目源码
  - `git clone https://github.com/Claymoreterasa/xdu-cloudcourse-spark.git`
  - [打包下载ZIP](https://github.com/Claymoreterasa/xdu-cloudcourse-spark/archive/master.zip)

## 导入项目

使用IntelliJ IDEA导入此项目，导入时使用Maven作为构建工具，之后下一步到完成即可。导入后需要下载依赖包，先确保电脑联网状态。如未自动下载，可以点击右侧`Maven Project`的`Reimport`继续下载依赖包。

## 配置Hbase节点

在HbaseConf类中修改Hbase节点地址

## 配置Spark节点

在SC类中修改Spark节点地址

## 运行项目

在VehicleCount类中右键 Run VehicleCount.main()[Ctrl-Shift-F10]

运行成功后，可以看到hbase web ui中，Tables表中出现了VehicleCount表。

在hbase shell中可以查看该表的记录（http://www.yiibai.com/hbase/hbase_shell.html）

## 使用项目

此项目实现了一个简单的地点过车统计功能，即从Hbase数据库Record表中查询出数据，统计每个地点的过车次数，并把结果写入VehicleCount表中

## 开发项目
### 1.  基础 （过车次数统计）[已实现]

#### 思路

从hbase Record表中读取所有记录，按地点分组，统计每组内的数量。

#### 表格式

表名：VehicleCount

|行健|列族|列1|列2|列3|列4|
|---|---|---|---|---|---
|placeId|info|address|latitude|longitude|count

#### 查询结果
命令行展示：hbase shell中能查询出记录

前端展示（加分）：获取每个地点的过车数量，（在地图上标记每个地点的过车数量和地址等信息[进阶]），需要在xdu-cloudcourse-web实现hbase查询接口，并实现一个页面，以表格显示查询结果

### 2. 加分（相遇次数统计）

#### 相遇定义

两车之间出现在同一地点的时间间隔小于一分钟

#### 思路

从hbase Record表中读取所有记录
1. 自身以地点为键进行join操作，计算除自身外的车辆是否相遇
```
(placeId，(eid，time)) join (placeId，(eid，time)) ->
(placeId, (eid1, time1, eid2, time2)) filter (eid1 != eid2 && |time1 - time2| < 60) ->
((eid1, eid2), 1) reduceByKey ->
((eid1, eid2), count)
```
2. 以地点为键进行分组，同一组内的数据按照时间进行排序，遍历整个列表，看哪些车辆之间满足时间间隔

#### 表格式

表名：MeetCount

|行健|列族|列|值|
|---|---|---|---
|车辆编号(eid)|info|相遇车辆编号(eid)|相遇次数

#### 查询结果
命令行展示：hbase shell中能查询出记录

前端展示（加分）：指定车辆编号，查询它相遇的所有车辆编号和相遇次数， 需要在xdu-cloudcourse-web实现hbase查询接口，并实现一个页面，以表格显示查询结果


## **Q&A**

### 1 依赖下载失败

检查网络连接，并多次尝试`Maven Reimport`。

### 2 VehicleCount运行无反应
如果运行程序后，输入下面三行文字就停住不动，请检查本机上hosts中是否配置了集群中hbase集群的ip地址,并保证本机防火墙关闭，且能ping通集群

windows 文件地址为 C:\Windows\System32\drivers\etc\hosts

centos 文件地址为 /etc/hosts
```
log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
```

### **其他未列错误或问题，请先自行尝试搜索引擎解决。**

# 项目涉及的技术说明

## [Spark](http://spark.apache.org/)

Spark是一个基于内存计算的分布式计算框架，被广泛使用

### 文档和教程

- <http://spark.apache.org/docs/latest/quick-start.html>
- <http://spark.apache.org/docs/latest/rdd-programming-guide.html>
- <http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package>
- <http://spark.apache.org/docs/latest/api/java/index.html>


## [HBase](https://hbase.apache.org/)

HBase是一个分布式，版本化，面向列的数据库，构建在 Apache Hadoop和 Apache ZooKeeper之上。

### 文档和教程

- <http://blog.csdn.net/u013468917/article/details/52822074>
- <http://hbase.apache.org/apidocs/>
