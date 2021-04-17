# 数据采集模块

#### 集群所有进程查看脚本

编写xcall.sh

```sh
#! /bin/bash
 
for i in hadoop102 hadoop103 hadoop104
do
    echo --------- $i ----------
    ssh $i "$*"
done
```

#### 以完全分布式安装Hadoop

1. 集群部署规划

   注意：NameNode和SecondaryNameNode不要安装在同一台服务器。

   注意：ResourceManager也很消耗内存，不要和NameNode、SecondaryNameNode配置在同一台机器上。

2. 解压hadoop包

3. 添加Hadoop环境变量

4. 分发环境变量

##### 配置集群

1. 核心配置文件 core-site.xml

   ```xml
   <!-- 指定NameNode的地址 -->
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://hadoop102:9820</value>
   </property>
   <!-- 指定hadoop数据的存储目录 -->
       <property>
           <name>hadoop.tmp.dir</name>
           <value>/opt/module/hadoop-3.1.3/data</value>
   </property>
   
   <!-- 配置HDFS网页登录使用的静态用户为atguigu -->
       <property>
           <name>hadoop.http.staticuser.user</name>
           <value>atguigu</value>
   </property>
   
   <!-- 配置该atguigu(superUser)允许通过代理访问的主机节点 -->
       <property>
           <name>hadoop.proxyuser.atguigu.hosts</name>
           <value>*</value>
   </property>
   <!-- 配置该atguigu(superUser)允许通过代理用户所属组 -->
       <property>
           <name>hadoop.proxyuser.atguigu.groups</name>
           <value>*</value>
   </property>
   <!-- 配置该atguigu(superUser)允许通过代理的用户-->
       <property>
           <name>hadoop.proxyuser.atguigu.groups</name>
           <value>*</value>
   </property>
   ```

2. HDFS配置文件 hdfs-site.xml

   ```xml
   <!-- nn web端访问地址-->
   	<property>
           <name>dfs.namenode.http-address</name>
           <value>hadoop102:9870</value>
       </property>
       
   	<!-- 2nn web端访问地址-->
       <property>
           <name>dfs.namenode.secondary.http-address</name>
           <value>hadoop104:9868</value>
       </property>
       
       <!-- 测试环境指定HDFS副本的数量1 -->
       <property>
           <name>dfs.replication</name>
           <value>1</value>
       </property>
   ```

3. YARN配置文件

   ```xml
   <!-- 指定MR走shuffle -->
       <property>
           <name>yarn.nodemanager.aux-services</name>
           <value>mapreduce_shuffle</value>
       </property>
       
       <!-- 指定ResourceManager的地址-->
       <property>
           <name>yarn.resourcemanager.hostname</name>
           <value>hadoop103</value>
       </property>
       
       <!-- 环境变量的继承 -->
       <property>
           <name>yarn.nodemanager.env-whitelist</name>
           <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
       </property>
       
       <!-- yarn容器允许分配的最大最小内存 -->
       <property>
           <name>yarn.scheduler.minimum-allocation-mb</name>
           <value>512</value>
       </property>
       <property>
           <name>yarn.scheduler.maximum-allocation-mb</name>
           <value>4096</value>
       </property>
       
       <!-- yarn容器允许管理的物理内存大小 -->
       <property>
           <name>yarn.nodemanager.resource.memory-mb</name>
           <value>4096</value>
       </property>
       
       <!-- 关闭yarn对物理内存和虚拟内存的限制检查 -->
       <property>
           <name>yarn.nodemanager.pmem-check-enabled</name>
           <value>false</value>
       </property>
       <property>
           <name>yarn.nodemanager.vmem-check-enabled</name>
           <value>false</value>
       </property>
   ```

4. MapReduce配置文件

   ```xml
   <!-- 指定MapReduce程序运行在Yarn上 -->
       <property>
           <name>mapreduce.framework.name</name>
           <value>yarn</value>
       </property>
   ```

5. 配置workers

   vim etc/hadoop/workers

   ```
   hadoop102
   hadoop103
   hadoop104
   ```

6. 配置历史服务器

   历史服务器用来查看程序的历史运行情况，需要配置历史服务器

   vim mapred-site.xml

   ```xml
   <!-- 历史服务器端地址 -->
   <property>
       <name>mapreduce.jobhistory.address</name>
       <value>hadoop102:10020</value>
   </property>
   
   <!-- 历史服务器web端地址 -->
   <property>
       <name>mapreduce.jobhistory.webapp.address</name>
       <value>hadoop102:19888</value>
   </property>
   ```

7. 配置日志的聚集

   日志聚集：应用运行完成以后，将程序运行日志信息上传到HDFS系统上

   好处：可以方便的查看到程序运行详情，方便开发调试

   vim yarn-site.xml

   ```xml
   <!-- 开启日志聚集功能 -->
   <property>
       <name>yarn.log-aggregation-enable</name>
       <value>true</value>
   </property>
   
   <!-- 设置日志聚集服务器地址 -->
   <property>  
       <name>yarn.log.server.url</name>  
       <value>http://hadoop102:19888/jobhistory/logs</value>
   </property>
   
   <!-- 设置日志保留时间为7天 -->
   <property>
       <name>yarn.log-aggregation.retain-seconds</name>
       <value>604800</value>
   </property>
   ```

##### hadoop群起脚本

hdp.sh

```xml
#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="

        echo " --------------- 启动 hdfs ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/sbin/start-dfs.sh"
        echo " --------------- 启动 yarn ---------------"
        ssh hadoop103 "/opt/module/hadoop-3.1.3/sbin/start-yarn.sh"
        echo " --------------- 启动 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="

        echo " --------------- 关闭 historyserver ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 yarn ---------------"
        ssh hadoop103 "/opt/module/hadoop-3.1.3/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop102 "/opt/module/hadoop-3.1.3/sbin/stop-dfs.sh"
;;
*)
    echo "Input Args Error..."
;;
esac
```

##### 集群时间同步

###### 时间服务器配置

1. 关闭ntp服务和自启动

   ```
   sudo systemctl stop ntpd
   sudo systemctl disable ntpd
   ```

2. 修改hadoop102的ntp.conf配置文件

   a）修改1（授权192.168.1.0-192.168.1.255网段上的所有机器可以从这台机器上查询和同步时间）

   \#restrict 192.168.1.0 mask 255.255.255.0 nomodify notrap

   为restrict 192.168.1.0 mask 255.255.255.0 nomodify notrap

   ​    b）修改2（集群在局域网中，不使用其他互联网上的时间）

   server 0.centos.pool.ntp.org iburst

   server 1.centos.pool.ntp.org iburst

   server 2.centos.pool.ntp.org iburst

   server 3.centos.pool.ntp.org iburst

   为

   **#**server 0.centos.pool.ntp.org iburst

   **#**server 1.centos.pool.ntp.org iburst

   **#**server 2.centos.pool.ntp.org iburst

   **#**server 3.centos.pool.ntp.org iburst

   c）添加3（当该节点丢失网络连接，依然可以采用本地时间作为时间服务器为集群中的其他节点提供时间同步）

   server 127.127.1.0

   fudge 127.127.1.0 stratum 10

3. 修改/etc/sysconfig/ntpd文件

   SYNC_HWCLOC=yes

4. 重新启动ntpd服务

5. 设置ntpd服务开机启动

###### 其他机器配置

（1）在其他机器配置10分钟与时间服务器同步一次

sudo crontab -e

编写定时任务如下：

*/10 * * * * /usr/sbin/ntpdate hadoop102

（2）修改任意机器时间

sudo date -s "2017-9-11 11:11:11"

（3）十分钟后查看机器是否与时间服务器同步

sudo date

说明：测试的时候可以将10分钟调整为1分钟，节省时间。

#### 支持LZO配置

1. hadoop本身不支持lzo压缩，故需要使用twitter提供的hadoop-lzo开源组件。hadoop-lzo需要依赖hadoop和lzo进行[编译](./lzo配置.md)

2. 将编译好的hadoop-lzo放入hadoop/share/hadoop/common

3. 同步hadoop-lzo

4. core-site.xml增加配置支持lzo压缩

   ```xml
       <property>
           <name>io.compression.codecs</name>
           <value>
               org.apache.hadoop.io.compress.GzipCodec,
               org.apache.hadoop.io.compress.DefaultCodec,
               org.apache.hadoop.io.compress.BZip2Codec,
               org.apache.hadoop.io.compress.SnappyCodec,
               com.hadoop.compression.lzo.LzoCodec,
               com.hadoop.compression.lzo.LzopCodec
           </value>
       </property>
   
       <property>
           <name>io.compression.codec.lzo.class</name>
           <value>com.hadoop.compression.lzo.LzoCodec</value>
       </property>
   ```

5. 同步core-site.xml

#### LZO创建索引

1. 手动为lzo压缩文件创建索引

   ```xml
   hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar  com.hadoop.compression.lzo.DistributedLzoIndexer /input/bigtable.lzo
   ```

2. 执行wordcount

#### Hadoop基准测试
```
\# 写文件
hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -write -nrFiles 10 -fileSize 128MB
\# 文件数量

2021-04-14 02:45:27,466 INFO fs.TestDFSIO:     Number of files: 10

总共文件大小

2021-04-14 02:45:27,466 INFO fs.TestDFSIO: Total MBytes processed: 1280

\# 吞吐量

2021-04-14 02:45:27,466 INFO fs.TestDFSIO:    Throughput mb/sec: 17.33

\# 平均IO速率

2021-04-14 02:45:27,466 INFO fs.TestDFSIO: Average IO rate mb/sec: 25.34

IO速率标准偏差

2021-04-14 02:45:27,466 INFO fs.TestDFSIO:  IO rate std deviation: 21.85

2021-04-14 02:45:27,466 INFO fs.TestDFSIO:   Test exec time sec: 69.62



\# 读文件
hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -read -nrFiles 10 -fileSize 128MB

2021-04-14 02:49:08,369 INFO fs.TestDFSIO:     Number of files: 10

2021-04-14 02:49:08,369 INFO fs.TestDFSIO: Total MBytes processed: 1280

2021-04-14 02:49:08,369 INFO fs.TestDFSIO:    Throughput mb/sec: 36.51

2021-04-14 02:49:08,369 INFO fs.TestDFSIO: Average IO rate mb/sec: 112.35

2021-04-14 02:49:08,369 INFO fs.TestDFSIO:  IO rate std deviation: 119.89

2021-04-14 02:49:08,369 INFO fs.TestDFSIO:   Test exec time sec: 52.39
```

#### 安装Zookeeper

1. 解压zookeeper

2. 创建zkData/myid文件

3. 在文件中添加与server对应的编号

4. 修改其他服务器的myid

5. 配置zoo.cfg

   ```xml
   dataDir=/opt/module/zookeeper-3.5.7/zkData
   
   #######################cluster##########################
   server.2=hadoop102:2888:3888
   server.3=hadoop103:2888:3888
   server.4=hadoop104:2888:3888
   
   server.A=B:C:D
   
   A是一个数字，表示这个是第几号服务器；
   集群模式下配置一个文件myid，这个文件在dataDir目录下，这个文件里面有一个数据就是A的值，Zookeeper启动时读取此文件，拿到里面的数据与zoo.cfg里面的配置信息比较从而判断到底是哪个server。
   B是这个服务器的地址；
   C是这个服务器Follower与集群中的Leader服务器交换信息的端口；
   D是万一集群中的Leader服务器挂了，需要一个端口来重新进行选举，选出一个新的Leader，而这个端口就是用来执行选举时服务器相互通信的端口。
   ```

6. 同步zoo.cfg配置文件

#### zookeeper集群启动停止脚本zk.sh

```xml
#!/bin/bash

case $1 in
"start"){
	for i in hadoop102 hadoop103 hadoop104
	do
        echo ---------- zookeeper $i 启动 ------------
		ssh $i "/opt/module/zookeeper-3.5.7/bin/zkServer.sh start"
	done
};;
"stop"){
	for i in hadoop102 hadoop103 hadoop104
	do
        echo ---------- zookeeper $i 停止 ------------    
		ssh $i "/opt/module/zookeeper-3.5.7/bin/zkServer.sh stop"
	done
};;
"status"){
	for i in hadoop102 hadoop103 hadoop104
	do
        echo ---------- zookeeper $i 状态 ------------    
		ssh $i "/opt/module/zookeeper-3.5.7/bin/zkServer.sh status"
	done
};;
esac
```

#### Kafka安装

1. 解压kafka

2. 创建logs文件夹

3. 修改配置文件

   ```xml
   cd config/
   vi server.properties
   
   修改或者增加以下内容：
   #broker的全局唯一编号，不能重复
   broker.id=0
   #删除topic功能使能
   delete.topic.enable=true
   #kafka运行日志存放的路径
   log.dirs=/opt/module/kafka/data
   #配置连接Zookeeper集群地址
   zookeeper.connect=hadoop102:2181,hadoop103:2181,hadoop104:2181/kafka
   ```

4. 配置环境变量

5. 分发server.properties，修改broker.id=1，broker.id=2

#### 创建kafka脚本kf.sh

```xml
#!/bin/bash

case $1 in
"start"){
    for i in hadoop102 hadoop103 hadoop104
    do
        echo " --------启动 $i Kafka-------"
        ssh $i "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties "
    done
};;
"stop"){
    for i in hadoop102 hadoop103 hadoop104
    do
        echo " --------停止 $i Kafka-------"
        ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh stop"
    done
};;
esac
```

#### Kafka监控

1. 修改kafka命令kafka-server-start.sh

   ```xml
   if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
       export KAFKA_HEAP_OPTS="-server -Xms2G -Xmx2G -XX:PermSize=128m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=8 -XX:ConcGCThreads=5 -XX:InitiatingHeapOccupancyPercent=70"
       export JMX_PORT="9999"
       #export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
   fi
   ```

2. 分发Kafka给其他节点

3. 解压kafka-eagle-bin

4. 将kafka-eagle-web解压

5. 修改配置文件

   ```xml
   ######################################
   # multi zookeeper&kafka cluster list
   ######################################
   kafka.eagle.zk.cluster.alias=cluster1
   cluster1.zk.list=hadoop102:2181,hadoop103:2181,hadoop104:2181
   
   ######################################
   # kafka offset storage
   ######################################
   cluster1.kafka.eagle.offset.storage=kafka
   
   ######################################
   # enable kafka metrics
   ######################################
   kafka.eagle.metrics.charts=true
   kafka.eagle.sql.fix.error=false
   
   ######################################
   # kafka jdbc driver address
   ######################################
   kafka.eagle.driver=com.mysql.jdbc.Driver
   kafka.eagle.url=jdbc:mysql://hadoop102:3306/ke?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
   kafka.eagle.username=root
   kafka.eagle.password=000000
   ```

6. 添加环境变量

7. 启动  bin/ke.sh start

8. [查看监控数据](http://192.168.9.102:8048/ke)

9. Kafka集群启动停止脚本

   ```xml
   #! /bin/bash
   
   case $1 in
   "start"){
       for i in hadoop102 hadoop103 hadoop104
       do
           echo " --------启动 $i Kafka-------"
           ssh $i "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties"
       done
   };;
   "stop"){
       for i in hadoop102 hadoop103 hadoop104
       do
           echo " --------停止 $i Kafka-------"
           ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh stop"
       done
   };;
   esac
   ```

#### Kafka压力测试

1. Producer压力测试

   ```xml
   100000 records sent, 30553.009471 records/sec (2.91 MB/sec), 1416.15 ms avg latency, 2222.00 ms max latency, 1439 ms 50th, 2127 ms 95th, 2202 ms 99th, 2222 ms 99.9th.
   ```

   写入10w条消息，吞吐量为2.91MB/sec，每次写入的平均延迟为1416.15ms，最大的延迟为2222ms。

2. Consumer压力测试

   ```
   消费数据：9.5367      吞吐量：0.6319      共消费多少条：100000  平均每秒消费6626.4661条。
   ```

3. Kafka分区数计算

   * 创建只有1个分区的topic
   * 测试这个topic的producer吞吐量和consumer吞吐量
   * 假设值分别为Tp和Tc，单位可以是MB/S
   * 总的目标吞吐量是Tt，那么分区数=Tt/min(Tp, Tc)

#### Flume安装

##### 解压Flume

将lib文件夹下的guava-11.0.2.jar删除以兼容Hadoop3.1.3

#### 修改Flume-env.sh

```xml
export JAVA_HOME=/opt/module/jdk1.8.0_212
```

#### 日志采集Flume配置

vim file-flume-kafka.conf

```xml
#为各组件命名
a1.sources = r1
a1.channels = c1

#描述source
a1.sources.r1.type = TAILDIR
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /opt/module/applog/log/app.*
a1.sources.r1.positionFile = /opt/module/flume/taildir_position.json
a1.sources.r1.interceptors =  i1
a1.sources.r1.interceptors.i1.type = com.atguigu.flume.interceptor.ETLInterceptor$Builder

#描述channel
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = hadoop102:9092,hadoop103:9092
a1.channels.c1.kafka.topic = topic_log
a1.channels.c1.parseAsFlumeEvent = false

#绑定source和channel以及sink和channel的关系
a1.sources.r1.channels = c1
```

#### Flume拦截器

1. 创建Maven工程flume-interceptor

2. 在pom.xml文件中添加依赖

   ```xml
   <dependency>
           <groupId>org.apache.flume</groupId>
           <artifactId>flume-ng-core</artifactId>
           <version>1.9.0</version>
           <scope>provided</scope>
       </dependency>
   
       <dependency>
           <groupId>com.alibaba</groupId>
           <artifactId>fastjson</artifactId>
           <version>1.2.62</version>
       </dependency>
   </dependencies>
   
   <plugins>
           <plugin>
               <artifactId>maven-compiler-plugin</artifactId>
               <version>2.3.2</version>
               <configuration>
                   <source>1.8</source>
                   <target>1.8</target>
               </configuration>
           </plugin>
           <plugin>
               <artifactId>maven-assembly-plugin</artifactId>
               <configuration>
                   <descriptorRefs>
                       <descriptorRef>jar-with-dependencies</descriptorRef>
                   </descriptorRefs>
               </configuration>
               <executions>
                   <execution>
                       <id>make-assembly</id>
                       <phase>package</phase>
                       <goals>
                           <goal>single</goal>
                       </goals>
                   </execution>
               </executions>
           </plugin>
   </plugins>
   
   ```

3. 包下创建JSONUtils类

   ```xml
   import com.alibaba.fastjson.JSON;
   import com.alibaba.fastjson.JSONException;
   
   public class JSONUtils {
       public static boolean isJSONValidate(String log){
           try {
               JSON.parse(log);
               return true;
           }catch (JSONException e){
               return false;
           }
       }
   }
   ```

4. 包下创建LogInterceptor类

   ```java
   import com.alibaba.fastjson.JSON;
   import org.apache.flume.Context;
   import org.apache.flume.Event;
   import org.apache.flume.interceptor.Interceptor;
   
   import java.nio.charset.StandardCharsets;
   import java.util.Iterator;
   import java.util.List;
   
   public class ETLInterceptor implements Interceptor {
   
       @Override
       public void initialize() {
   
       }
   
       @Override
       public Event intercept(Event event) {
   
           byte[] body = event.getBody();
           String log = new String(body, StandardCharsets.UTF_8);
   
           if (JSONUtils.isJSONValidate(log)) {
               return event;
           } else {
               return null;
           }
       }
   
       @Override
       public List<Event> intercept(List<Event> list) {
   
           Iterator<Event> iterator = list.iterator();
   
           while (iterator.hasNext()){
               Event next = iterator.next();
               if(intercept(next)==null){
                   iterator.remove();
               }
           }
   
           return list;
       }
   
       public static class Builder implements Interceptor.Builder{
   
           @Override
           public Interceptor build() {
               return new ETLInterceptor();
           }
           @Override
           public void configure(Context context) {
   
           }
   
       }
   
       @Override
       public void close() {
   
       }
   }
   ```

5. 打包并分发

##### Flume启动停止脚本 f1.sh

```xml
#! /bin/bash

case $1 in
"start"){
        for i in hadoop102 hadoop103
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /opt/module/flume/bin/flume-ng agent --conf-file /opt/module/flume/conf/file-flume-kafka.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/module/flume/log1.txt 2>&1  &"
        done
};;	
"stop"){
        for i in hadoop102 hadoop103
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep file-flume-kafka | grep -v grep |awk  '{print \$2}' | xargs -n1 kill -9 "
        done

};;
esac
```

##### 消费Flume配置

vim kafka-flume-hdfs.conf

```xml
## 组件
a1.sources=r1
a1.channels=c1
a1.sinks=k1

## source1
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000
a1.sources.r1.kafka.bootstrap.servers = hadoop102:9092,hadoop103:9092,hadoop104:9092
a1.sources.r1.kafka.topics=topic_log
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.atguigu.flume.interceptor.TimeStampInterceptor$Builder

## channel1
a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /opt/module/flume/checkpoint/behavior1
a1.channels.c1.dataDirs = /opt/module/flume/data/behavior1/
a1.channels.c1.maxFileSize = 2146435071
a1.channels.c1.capacity = 1000000
a1.channels.c1.keep-alive = 6


## sink1
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/log/topic_log/%Y-%m-%d
a1.sinks.k1.hdfs.filePrefix = log-
a1.sinks.k1.hdfs.round = false


a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0

## 控制输出文件是原生文件。
a1.sinks.k1.hdfs.fileType = CompressedStream
a1.sinks.k1.hdfs.codeC = lzop

## 拼装
a1.sources.r1.channels = c1
a1.sinks.k1.channel= c1
```

##### 编写Flume拦截器

```xml
package com.atguigu.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeStampInterceptor implements Interceptor {

    private ArrayList<Event> events = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        String ts = jsonObject.getString("ts");
        headers.put("timestamp", ts);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
```

##### 打包放入flume的lib文件夹下

#### 创建消费Flume脚本

```xml
#! /bin/bash

case $1 in
"start"){
        for i in hadoop104
        do
                echo " --------启动 $i 消费flume-------"
                ssh $i "nohup /opt/module/flume/bin/flume-ng agent --conf-file /opt/module/flume/conf/kafka-flume-hdfs.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/module/flume/log2.txt   2>&1 &"
        done
};;
"stop"){
        for i in hadoop104
        do
                echo " --------停止 $i 消费flume-------"
                ssh $i "ps -ef | grep kafka-flume-hdfs | grep -v grep |awk '{print \$2}' | xargs -n1 kill"
        done

};;
esac
```

#### 采集通道启动停止脚本

vim cluster.sh

```sh
#!/bin/bash

case $1 in
"start"){
        echo ================== 启动 集群 ==================

        #启动 Zookeeper集群
        zk.sh start

        #启动 Hadoop集群
        hdp.sh start

        #启动 Kafka采集集群
        kf.sh start

        #启动 Flume采集集群
        f1.sh start

        #启动 Flume消费集群
        f2.sh start

        };;
"stop"){
        echo ================== 停止 集群 ==================

        #停止 Flume消费集群
        f2.sh stop

        #停止 Flume采集集群
        f1.sh stop

        #停止 Kafka采集集群
        kf.sh stop

        #停止 Hadoop集群
        hdp.sh stop

        #停止 Zookeeper集群
        zk.sh stop

};;
esac
```

