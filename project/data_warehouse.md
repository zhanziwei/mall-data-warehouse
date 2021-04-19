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

* 对用户行为数据解析

* 对核心数据进行判空过滤

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
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.start.entry'),
       get_json_object(line,'$.start.loading_time'),
       get_json_object(line,'$.start.open_ad_id'),
       get_json_object(line,'$.start.open_ad_ms'),
       get_json_object(line,'$.start.open_ad_skip_ms'),
       get_json_object(line,'$.ts')
   from ${APP}.ods_log
   where dt='$do_date' and get_json_object(line,'$.start') is not null;
   
   
   insert overwrite table ${APP}.dwd_action_log
   partition(dt='$do_date')
   select
   	get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.page.during_time'),
       get_json_object(line,'$.page.item'),
       get_json_object(line,'$.page.item_type'),
       get_json_object(line,'$.page.last_page_id'),
       get_json_object(line,'$.page.page_id'),
       get_json_object(line,'$.page.sourceType'),
       get_json_object(action,'$.action_id'),
       get_json_object(action,'$.item'),
       get_json_object(action,'$.item_type'),
       get_json_object(action,'$.ts')
   from ${APP}.ods_log lateral view 
   ${APP}.explode_json_array(get_json_object(line,'$.actions')) tmp as action
   where dt='$do_date'
   and get_json_object(line, '$.actions') is not null;
   
   insert overwrite table ${APP}.dwd_display_log partition(dt='$do_date')
   select
       get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.page.during_time'),
       get_json_object(line,'$.page.item'),
       get_json_object(line,'$.page.item_type'),
       get_json_object(line,'$.page.last_page_id'),
       get_json_object(line,'$.page.page_id'),
       get_json_object(line,'$.page.sourceType'),
       get_json_object(line,'$.ts'),
       get_json_object(display,'$.displayType'),
       get_json_object(display,'$.item'),
       get_json_object(display,'$.item_type'),
       get_json_object(display,'$.order')
   from ${APP}.ods_log lateral view ${APP}.explode_json_array(get_json_object(line,'$.displays')) tmp as display
   where dt='$do_date'
   and get_json_object(line,'$.displays') is not null;
   
   insert overwrite table ${APP}.dwd_page_log partition(dt='$do_date')
   select
       get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.page.during_time'),
       get_json_object(line,'$.page.item'),
       get_json_object(line,'$.page.item_type'),
       get_json_object(line,'$.page.last_page_id'),
       get_json_object(line,'$.page.page_id'),
       get_json_object(line,'$.page.sourceType'),
       get_json_object(line,'$.ts')
   from ${APP}.ods_log
   where dt='$do_date'
   and get_json_object(line,'$.page') is not null;
   
   
   insert overwrite table ${APP}.dwd_error_log partition(dt='$do_date')
   select
       get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.page.item'),
       get_json_object(line,'$.page.item_type'),
       get_json_object(line,'$.page.last_page_id'),
       get_json_object(line,'$.page.page_id'),
       get_json_object(line,'$.page.sourceType'),
       get_json_object(line,'$.start.entry'),
       get_json_object(line,'$.start.loading_time'),
       get_json_object(line,'$.start.open_ad_id'),
       get_json_object(line,'$.start.open_ad_ms'),
       get_json_object(line,'$.start.open_ad_skip_ms'),
       get_json_object(line,'$.actions'),
       get_json_object(line,'$.displays'),
       get_json_object(line,'$.ts'),
       get_json_object(line,'$.err.error_code'),
       get_json_object(line,'$.err.msg')
   from ${APP}.ods_log 
   where dt='$do_date'
   and get_json_object(line,'$.err') is not null;
   "
   
   $hive -e "$sql"
   ```

#### DWS层

1. 每日设备行为

   ```sql
   drop table if exists dws_uv_detail_daycount;
   
   create external table dws_uv_detail_daycount (
   	`mid_id` string comment '设备id',
       `brand` string comment '手机品牌',
       `model` string comment '手机型号',
       `login_count` bigint comment '活跃次数',
       `page_stats` array<struct<page_id:string,page_count:bigint>> comment '页面访问统计'
   ) comment '每日设备行为表'
   partitioned by (dt string)
   stored as parquet
   location '/warehouse/gmall/dws/dws_uv_detail_daycount'
   tblproperties ("parquet.compression"="lzo");
   ```

2. 数据加载脚本  dwd_to_dws.sh

   ```sh
   #!/bin/bash
   
   APP=gmall
   hive=/opt/module/hive/bin/hive
   
   if [ -n "$1" ]
   then
   	do_date=$1
   else
   	do_date=`date -d "-1 day"+%F`
   fi
   
   sql="
   set mapreduce.job.queuename=hive;
   with
   tmp_start as
   (
   	select 
   		mid_id,
   		brand,
   		model,
   		count(*) login_count
   	from ${APP}.dwd_start_log
   	where dt='$do_date'
   	group by mid_id,brand,model
   ),
   tmp_page as
   (
   	select
   		mid_id,
   		brand,
   		model,
   		collect_set(named_struct('page_id',page_id,'page_count',page_count)) page_stats
   	from
   	(
   		select
   			mid_id,
   			brand,
   			model,
   			page_id,
   			count(*) page_count
   		from ${APP}.dwd_page_log
   		where dt='$do_date'
   		group by mid_id,brand,model
   	) tmp
   	group by mid_id,brand,model
   )
   
   
   insert overwrite table ${APP}.dws_uv_detail_daycount partition(dt='$do_date')
   select
   	nvl(tmp_start.mid_id,tmp_page.mid_id),
   	nvl(tmp_start.brand,tmp_page.brand),
   	nvl(tmp_start.model,tmp_page.model),
   	tmp_start.login_count,
   	tmp_page.page_stats
   from tmp_start
   full outer join tmp_page
   on tmp_start.mid_id=tmp_page.mid_id
   and tmp_start.brand=tmp_page.brand
   and tmp_start.model=tmp_page.model;
   "
   
   $hive -e "$sql"
   ```

#### DWT层

1. 设备主题宽表

   ```sql
   drop table if exists dwt_uv_topic;
   create external table dwt_uv_topic (
   	`mid_id` string comment '设备id',
       `brand` string comment '手机品牌',
       `model` string comment '手机型号',
       `login_date_first` string comment '首次活跃时间',
       `login_date_last` string comment '末次活跃时间',
       `login_day_count` bigint comment '当日活跃次数',
       `login_count` bigint comment '累积活跃天数'
   ) comment '设备主题宽表'
   stored as parquet
   location '/warehouse/gmall/dwt/dwt_uv_topic'
   tblproperties ("parquet.compression"="lzo");
   ```

2. 数据导入脚本 dws_to_dwt.sh

   ```bash
   #!/bin/bash
   
   APP=gmall
   hive=/opt/module/hive/bin/hive
   
   if [ -n "$1" ]
   then
   	do_date=$1
   else
   	do_date=`date -d "-1 day" +%F`
   fi
   
   sql="
   set mapreduce.job.queuename=hive;
   insert overwrite table ${APP}.dwt_uv_topic
   select
   	nvl(new.mid_id,old.mid_id),
   	nvl(new.model,old.model),
   	nvl(new.brand,old.brand),
   	if(old.mid_id is null, '$do_date', old.login_date_first),
   	if(new.mid_id is not null, '$do_date', old.login_date_last),
   	if(new.mid_id is not null, new.login_count, 0),
   	nvl(old.login_count,0)+if(new.login_count>0,1,0)
   from
   (
   	select * from ${APP}.dwt_uv_topic
   ) old
   full outer join
   (
   	select * from ${APP}.dws_uv_detail_daycount
   	where dt='$do_date'
   ) new
   on old.mid_id=new.mid_id;
   "
   ```

#### ADS层

ADS层不涉及建模，建表根据具体需求而定

1. 活跃设备数

   ```sql
   drop table if exists ads_uv_count;
   create external table ads_uv_count(
       `dt` string COMMENT '统计日期',
       `day_count` bigint COMMENT '当日用户数量',
       `wk_count`  bigint COMMENT '当周用户数量',
       `mn_count`  bigint COMMENT '当月用户数量',
       `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
       `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 
   ) comment '活跃设备数'
   row format delimited fields terminated by '\t'
   location '/warehouse/gmall/ads/ads_uv_count/';
   
   
   insert into table ads_uv_count
   select
   	'2020-06-14' dt,
   	daycount.ct,
   	wkcount.ct,
   	mncount.ct,
   	if(date_add(next_day('2020-06-14', 'MO'),-1)='2020-06-14','Y','N'),
   	if(last_day('2020-06-14')='2020-06-14','Y','N')
   from
   (
   	select
       	'2020-06-14' dt,
       	count(*) ct
       from dwt_uv_topic
       where login_date_last='2020-06-14'
   ) daycount join
   (
   	select
       	'2020-06-14'
       	count(*) ct
       from dwt_uv_topic
       where login_date_last>=date_add(next_day('2020-06-14','MO'),-7)
       and login_date_last<=date_add(next_day('2020-06-14','MO'),-1)
   ) wkcount on daycount.dt=wkcount.dt
   join
   (
   	select
       	'2020-06-14' dt,
       	count(*) ct
      	from dwt_uv_topic
       where date_format(login_date_last,'yyyy-MM')=date_format('2020-06-14','yyyy-MM')
   ) mncount on daycount.dt=mncount.dt;
   ```

2. 每日新增设备

   ```sql
   drop table if exists ads_new_mid_count;
   create external table ads_new_mid_count(
   	`create_date` string comment '创建时间',
       `new_mid_count` bigint comment '新增设备数量'
   ) comment '每日新增设备数量'
   row format delimited fields terminated by '\t'
   location '/warehouse/gmall/ads/ads_new_mid_count/';
   
   insert into table ads_new_mid_count
   select
   	'2020-06-14',
   	count(*)
   from dwt_uv_topic
   where login_date_first='2020-06-14'
   ```

3. 本周回流用户数

   上周未活跃，本周活跃的设备，且不是本周新增设备

   ```sql
   drop table if exists ads_back_count;
   create external table ads_back_count( 
       `dt` string COMMENT '统计日期',
       `wk_dt` string COMMENT '统计日期所在周',
       `wastage_count` bigint COMMENT '回流设备数'
   ) COMMENT '本周回流用户数'
   row format delimited fields terminated by '\t'
   location '/warehouse/gmall/ads/ads_back_count';
   
   
   insert into table ads_back_count
   select
       '2020-06-25',
       concat(date_add(next_day('2020-06-25','MO'),-7),'_', date_add(next_day('2020-06-25','MO'),-1)),
       count(*)
   from
   (
       select
           mid_id
       from dwt_uv_topic
       where login_date_last>=date_add(next_day('2020-06-25','MO'),-7) 
       and login_date_last<= date_add(next_day('2020-06-25','MO'),-1)
       and login_date_first<date_add(next_day('2020-06-25','MO'),-7)
   )current_wk
   left join
   (
       select
           mid_id
       from dws_uv_detail_daycount
       where dt>=date_add(next_day('2020-06-25','MO'),-7*2) 
       and dt<= date_add(next_day('2020-06-25','MO'),-7-1) 
       group by mid_id
   )last_wk
   on current_wk.mid_id=last_wk.mid_id
   where last_wk.mid_id is null;
   ```

4. 流失用户数

   最近7天未活跃的设备

   ```sql
   drop table if exists ads_wastage_count;
   create external table ads_wastage_count( 
       `dt` string COMMENT '统计日期',
       `wastage_count` bigint COMMENT '流失设备数'
   ) COMMENT '流失用户数'
   row format delimited fields terminated by '\t'
   location '/warehouse/gmall/ads/ads_wastage_count';
   
   insert into table ads_wastage_count
   select
        '2020-06-25',
        count(*)
   from 
   (
       select 
           mid_id
       from dwt_uv_topic
       where login_date_last<=date_add('2020-06-25',-7)
       group by mid_id
   )t1;
   ```

5. 最近连续三周活跃用户数

   ```sql
   drop table if exists ads_continuity_wk_count;
   create external table ads_continuity_wk_count( 
       `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
       `wk_dt` string COMMENT '持续时间',
       `continuity_count` bigint COMMENT '活跃用户数'
   ) COMMENT '最近连续三周活跃用户数'
   row format delimited fields terminated by '\t'
   location '/warehouse/gmall/ads/ads_continuity_wk_count';
   
   
   insert into table ads_continuity_wk_count
   select
       '2020-06-25',
       concat(date_add(next_day('2020-06-25','MO'),-7*3),'_',date_add(next_day('2020-06-25','MO'),-1)),
       count(*)
   from
   (
       select
           mid_id
       from
       (
           select
               mid_id
           from dws_uv_detail_daycount
           where dt>=date_add(next_day('2020-06-25','monday'),-7)
           and dt<=date_add(next_day('2020-06-25','monday'),-1)
           group by mid_id
   
           union all
   
           select
               mid_id
           from dws_uv_detail_daycount
           where dt>=date_add(next_day('2020-06-25','monday'),-7*2)
           and dt<=date_add(next_day('2020-06-25','monday'),-7-1)
           group by mid_id
   
           union all
   
           select
               mid_id
           from dws_uv_detail_daycount
           where dt>=date_add(next_day('2020-06-25','monday'),-7*3)
           and dt<=date_add(next_day('2020-06-25','monday'),-7*2-1)
           group by mid_id
       )t1
       group by mid_id
       having count(*)=3
   )t2;
   ```

6. 最近七天内连续三天活跃用户数

   ```sql
   drop table if exists ads_continuity_uv_count;
   create external table ads_continuity_uv_count( 
       `dt` string COMMENT '统计日期',
       `wk_dt` string COMMENT '最近7天日期',
       `continuity_count` bigint
   ) COMMENT '最近七天内连续三天活跃用户数'
   row format delimited fields terminated by '\t'
   location '/warehouse/gmall/ads/ads_continuity_uv_count';
   
   
   insert into table ads_continuity_uv_count
   select
       '2020-06-16',
       concat(date_add('2020-06-16',-6),'_','2020-06-16'),
       count(*)
   from
   (
       select mid_id
       from
       (
           select mid_id
           from
           (
               select 
                   mid_id,
                   date_sub(dt,rank) date_dif
               from
               (
                   select
                       mid_id,
                       dt,
                       rank() over(partition by mid_id order by dt) rank
                   from dws_uv_detail_daycount
                   where dt>=date_add('2020-06-16',-6) and dt<='2020-06-16'
               )t1
           )t2 
           group by mid_id,date_dif
           having count(*)>=3
       )t3 
       group by mid_id
   )t4;
   ```

7. 导入脚本dwt_to_ads.sh

   把2020-06-25改成输入的date

