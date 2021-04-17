#### 将数仓分为5层

1. ODS层：Original Data Store 原始数据层
2. DWD层：Data warehouse detail 明细数据层
3. DWS层：Data warehouse service  服务数据层
4. DWT层：Data warehouse Topic  数据主题层
5. ADS层：Application Data Store  数据应用层

#### Hive的环境准备

使用Hive on Spark：Hive既作为存储元数据又负责SQL的解析优化，语法是HQL，执行引擎变成了Spark，Spark负责采用RDD执行。

##### Hive on Spark配置

1. 兼容性说明

   Hive3.1.2和spark3.0.0不兼容，因为Hive3.1.2支持的spark是2.4.5，因此要重新编译Hive3.1.2。

   下载Hive3.1.2源码，修改pom文件中引用的Spark版本为3.0.0，打包获取jar包。

2. 在Hive节点部署Spark

   解压并配置Spark环境变量

3. 在Hive中创建spark配置文件

   vim /opt/module/hive/conf/spark-defaults.conf

   ```xml
   spark.master                               yarn
   spark.eventLog.enabled                   true
   spark.eventLog.dir                        hdfs://hadoop102:9820/spark-history
   spark.executor.memory                    1g
   spark.driver.memory					   1g
   ```

4. 向HDFS上传Spark纯净版jar包，不包含hadoop和hive相关依赖，并上传至HDFS集群路径

   hadoop fs -mkdir /spark-jars

   hadoop fs -put spark-3.0.0-bin-without-hadoop/jars/* /spark-jars

5. 修改hive-site.xml文件

   ```
   <!--Spark依赖位置（注意：端口号9820必须和namenode的端口号一致）-->
   <property>
       <name>spark.yarn.jars</name>
       <value>hdfs://hadoop102:9820/spark-jars/*</value>
   </property>
     
   <!--Hive执行引擎-->
   <property>
       <name>hive.execution.engine</name>
       <value>spark</value>
   </property>
   
   <!--Hive和Spark连接超时时间-->
   <property>
       <name>hive.spark.client.connect.timeout</name>
       <value>10000ms</value>
   </property>
   ```

#### Yarn容量调度器并发

Yarn默认调度器为Capacity Scheduler，且默认只有一个队列--default。

1. 增加ApplicationMaster的资源比例

   ```xml
   <property>
       <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
       <value>0.5</value>
       <description>
         集群中用于运行应用程序ApplicationMaster的资源比例上限，
   该参数通常用于限制处于活动状态的应用程序数目。该参数类型为浮点型，
   默认是0.1，表示10%。所有队列的ApplicationMaster资源比例上限可通过参数
   yarn.scheduler.capacity.maximum-am-resource-percent设置，而单个队列可通过参数yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent设置适合自己的值。
       </description>
   </property>
   ```

2. 配置Yarn容量调度器多队列

   * 修改容量调度器配置文件

     ```xml
     <property>
         <name>yarn.scheduler.capacity.root.queues</name>
         <value>default,hive</value>
         <description>
          再增加一个hive队列
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.default.capacity</name>
     <value>50</value>
         <description>
           default队列的容量为50%
         </description>
     </property>
     ```

   * 为新加队列添加必要属性

     ```xml
     <property>
         <name>yarn.scheduler.capacity.root.hive.capacity</name>
     <value>50</value>
         <description>
           hive队列的容量为50%
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.user-limit-factor</name>
     <value>1</value>
         <description>
           一个用户最多能够获取该队列资源容量的比例，取值0-1
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.maximum-capacity</name>
     <value>80</value>
         <description>
           hive队列的最大容量（自己队列资源不够，可以使用其他队列资源上限）
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.state</name>
         <value>RUNNING</value>
         <description>
           开启hive队列运行，不设置队列不能使用
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.acl_submit_applications</name>
     <value>*</value>
         <description>
           访问控制，控制谁可以将任务提交到该队列,*表示任何人
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.acl_administer_queue</name>
     <value>*</value>
         <description>
           访问控制，控制谁可以管理(包括提交和取消)该队列的任务，*表示任何人
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.acl_application_max_priority</name>
     <value>*</value>
     <description>
           指定哪个用户可以提交配置任务优先级
         </description>
     </property>
     
     <property>
         <name>yarn.scheduler.capacity.root.hive.maximum-application-lifetime</name>
     <value>-1</value>
         <description>
           hive队列中任务的最大生命时长，以秒为单位。任何小于或等于零的值将被视为禁用。
     </description>
     </property>
     <property>
         <name>yarn.scheduler.capacity.root.hive.default-application-lifetime</name>
     <value>-1</value>
         <description>
           hive队列中任务的默认生命时长，以秒为单位。任何小于或等于零的值将被视为禁用。
     </description>
     </property>
     ```

   * 分发配置文件并重启Hadoop集群

#### 创建数据库

create database gmall;   use gmall;



#### ODS层

1. 保持数据原貌不做任何修改，起到备份数据的作用
2. 数据采用压缩，减少磁盘存储空间
3. 创建分区表，防止后续的全表扫描

##### 创建表

```sql
drop table if exists ods_log;
create external table ods_log (
	`line` string
) partitioned by (`dt` string)
STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_log'  -- 指定数据在hdfs上的存储位置
;
```

##### ODS层编写脚本 hdfs_to_ods_log.sh

```
#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/opt/module/hive/bin/hive
hadoop=/opt/module/hadoop-3.1.3/bin/hadoop

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ]
then
  do_date=$1
else
  do_date=`date -d "-1 day" + %F`
fi

echo ============================ 日志日期为 $do_date ===============
sql="
load data inpath '/origin_data/$APP/log/topic_log/$do_date' into table ${APP}.ods_log partition(dt='$do_date');
"

$hive -e "$sql"

$hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer -Dmapreduce.job.queuename=hive /warehouse/$APP/ods/ods_log/dt=$do_date
```

#### DWD层

1. 启动日志表

   ```sql
   drop table if exists dwd_start_log;
   create external table dwd_start_log(
   	`area_code` string comment '地区编码',
       `brand` string comment '手机品牌',
       `channel` string comment '渠道',
       `model` string comment '手机型号',
       `mid_id` string comment '设备id',
       `os` string comment '操作系统',
       `user_id` string comment '会员id',
       `version_code` string comment 'app版本号',
       `entry` string COMMENT ' icon手机图标  notice 通知   install 安装后启动',
       `loading_time` bigint COMMENT '启动加载时间',
       `open_ad_id` string COMMENT '广告页ID ',
       `open_ad_ms` bigint COMMENT '广告总共播放时间', 
       `open_ad_skip_ms` bigint COMMENT '用户跳过广告时点', 
       `ts` bigint COMMENT '时间'
   ) comment '启动日志表'
   PARTITIONED BY (dt string) -- 按照时间创建分区
   stored as parquet -- 采用parquet列式存储
   LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在HDFS上存储位置
   TBLPROPERTIES('parquet.compression'='lzo') -- 采用LZO压缩
   ;
   ```

2. Hive读取索引文件问题

   默认的CombineHiveInputFormat不能识别lzo索引文件，将索引文件当作普通文件处理，且导致LZO文件无法切片。

   set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

3. 页面日志表

   ```
   drop table if exists dwd_page_log;
   create external table dwd_page_log(
       `area_code` string COMMENT '地区编码',
       `brand` string COMMENT '手机品牌', 
       `channel` string COMMENT '渠道', 
       `model` string COMMENT '手机型号', 
       `mid_id` string COMMENT '设备id', 
       `os` string COMMENT '操作系统', 
       `user_id` string COMMENT '会员id', 
       `version_code` string COMMENT 'app版本号', 
       `during_time` bigint COMMENT '持续时间毫秒',
       `page_item` string COMMENT '目标id ', 
       `page_item_type` string COMMENT '目标类型', 
       `last_page_id` string COMMENT '上页类型', 
       `page_id` string COMMENT '页面ID ',
       `source_type` string COMMENT '来源类型', 
       `ts` bigint
   ) comment '页面日志表'
   partitioned by (dt string)
   ```

4. 动作日志表

   ```sql
   drop table if exists dwd_action_log;
   create external table dwd_action_log(
       `area_code` string COMMENT '地区编码',
       `brand` string COMMENT '手机品牌', 
       `channel` string COMMENT '渠道', 
       `model` string COMMENT '手机型号', 
       `mid_id` string COMMENT '设备id', 
       `os` string COMMENT '操作系统', 
       `user_id` string COMMENT '会员id', 
       `version_code` string COMMENT 'app版本号', 
       `during_time` bigint COMMENT '持续时间毫秒', 
       `page_item` string COMMENT '目标id ', 
       `page_item_type` string COMMENT '目标类型', 
       `last_page_id` string COMMENT '上页类型', 
       `page_id` string COMMENT '页面id ',
       `source_type` string COMMENT '来源类型', 
       `action_id` string COMMENT '动作id',
       `item` string COMMENT '目标id ',
       `item_type` string COMMENT '目标类型', 
       `ts` bigint COMMENT '时间'
   ) COMMENT '动作日志表'
   PARTITIONED BY (dt string)
   stored as parquet
   LOCATION '/warehouse/gmall/dwd/dwd_action_log'
   TBLPROPERTIES('parquet.compression'='lzo');
   ```

5. 创建UDTF函数

   * 创建一个Maven工程

   * 创建包名

   * 引入hive依赖

     ```sql
     <dependencies>
         <!--添加hive依赖-->
         <dependency>
             <groupId>org.apache.hive</groupId>
             <artifactId>hive-exec</artifactId>
             <version>3.1.2</version>
         </dependency>
     </dependencies>
     ```

   * 编码

     ```java
     import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
     import org.apache.hadoop.hive.ql.metadata.HiveException;
     import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
     import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
     import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
     import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
     import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
     import org.json.JSONArray;
     
     import java.util.ArrayList;
     import java.util.List;
     
     public class ExplodeJSONArray extends GenericUDTF {
     
         @Override
         public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
     
             // 1 参数合法性检查
             if (argOIs.getAllStructFieldRefs().size() != 1){
                 throw new UDFArgumentException("ExplodeJSONArray 只需要一个参数");
             }
     
             // 2 第一个参数必须为string
             if(!"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())){
                 throw new UDFArgumentException("json_array_to_struct_array的第1个参数应为string类型");
             }
     
             // 3 定义返回值名称和类型
             List<String> fieldNames = new ArrayList<String>();
             List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
     
             fieldNames.add("items");
             fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
     
             return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
         }
     
         public void process(Object[] objects) throws HiveException {
     
             // 1 获取传入的数据
             String jsonArray = objects[0].toString();
     
             // 2 将string转换为json数组
             JSONArray actions = new JSONArray(jsonArray);
             
             // 3 循环一次，取出数组中的一个json，并写出
             for (int i = 0; i < actions.length(); i++) {
     
                 String[] result = new String[1];
                 result[0] = actions.getString(i);
                 forward(result);
             }
         }
     
         public void close() throws HiveException {
     
         }
     }
     ```

   * 创建函数

     1）打包

     2）将hivefunction的jar包上传到hadoop102的/opt/module，然后将jar包上传给HDFS的/user/hive/jars路径

     3）创建永久函数与开发好的java class关联

     ```sql
     create function explode_json_array as 'com.atguigu.hive.udtf.ExplodeJSONArray' using jar 'hdfs://hadoop102:9820/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar';
     ```

6. 曝光日志表

   ```sql
   drop table if exists dwd_display_log;
   CREATE EXTERNAL TABLE dwd_display_log(
       `area_code` string COMMENT '地区编码',
       `brand` string COMMENT '手机品牌', 
       `channel` string COMMENT '渠道', 
       `model` string COMMENT '手机型号', 
       `mid_id` string COMMENT '设备id', 
       `os` string COMMENT '操作系统', 
       `user_id` string COMMENT '会员id', 
       `version_code` string COMMENT 'app版本号', 
       `during_time` bigint COMMENT 'app版本号',
       `page_item` string COMMENT '目标id ', 
       `page_item_type` string COMMENT '目标类型', 
       `last_page_id` string COMMENT '上页类型', 
       `page_id` string COMMENT '页面ID ',
       `source_type` string COMMENT '来源类型', 
       `ts` bigint COMMENT 'app版本号',
       `display_type` string COMMENT '曝光类型',
       `item` string COMMENT '曝光对象id ',
       `item_type` string COMMENT 'app版本号', 
       `order` bigint COMMENT '出现顺序'
   ) COMMENT '曝光日志表'
   PARTITIONED BY (dt string)
   stored as parquet
   LOCATION '/warehouse/gmall/dwd/dwd_display_log'
   TBLPROPERTIES('parquet.compression'='lzo');
   ```

7. 错误日志表

   ```sql
   drop table if exists dwd_error_log;
   CREATE EXTERNAL TABLE dwd_error_log(
       `area_code` string COMMENT '地区编码',
       `brand` string COMMENT '手机品牌', 
       `channel` string COMMENT '渠道', 
       `model` string COMMENT '手机型号', 
       `mid_id` string COMMENT '设备id', 
       `os` string COMMENT '操作系统', 
       `user_id` string COMMENT '会员id', 
       `version_code` string COMMENT 'app版本号', 
       `page_item` string COMMENT '目标id ', 
       `page_item_type` string COMMENT '目标类型', 
       `last_page_id` string COMMENT '上页类型', 
       `page_id` string COMMENT '页面ID ',
       `source_type` string COMMENT '来源类型', 
       `entry` string COMMENT ' icon手机图标  notice 通知 install 安装后启动',
       `loading_time` string COMMENT '启动加载时间',
       `open_ad_id` string COMMENT '广告页ID ',
       `open_ad_ms` string COMMENT '广告总共播放时间', 
       `open_ad_skip_ms` string COMMENT '用户跳过广告时点',
       `actions` string COMMENT '动作',
       `displays` string COMMENT '曝光',
       `ts` string COMMENT '时间',
       `error_code` string COMMENT '错误码',
       `msg` string COMMENT '错误信息'
   ) COMMENT '错误日志表'
   PARTITIONED BY (dt string)
   stored as parquet
   LOCATION '/warehouse/gmall/dwd/dwd_error_log'
   TBLPROPERTIES('parquet.compression'='lzo');
   ```

8. 数据加载脚本  ods_to_dwd_log.sh

   ```sql
   #!/bin/bash
   
   hive=/opt/module/hive/bin/hive
   APP=gmall
   
   if [ -n "$1" ]
   then
     do_date=$1
   else
     do_date=`date -d "-1 day" + %F`
   fi
   
   sql="
   set mapreduce.job.queuename=hive;
   set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
   "
   
   insert overwrite table ${APP}.dwd_start_log partition(dt='$do_date')
   select
     get_json_object(line,'$.common.ar'),
     get_json_object(line,'$.common.ba'),
     
    
   ```

   

