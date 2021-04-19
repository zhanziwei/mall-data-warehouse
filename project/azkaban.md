# Azkaban调度

### Azkaban部署

1. 解压Azkaban-db，Azkaban-exec，Azkaban-web的tar包

2. 配置MySQL

   * 启动mysql

      mysql -uroot -p000000

   * 创建Azkaban数据库

     create database azkaban;

   * 创建Azkaban用户并赋予权限

     set global validate_password_length=4;

     set global validate_password_policy=0;

     CREATE USER 'azkaban'@'%' IDENTIFIED BY '000000';

   * 创建Azkaban表

     use azkaban;

     source /opt/module/azkaban/azkaban-db-3.84.4/create-all-sql-3.84.4.sql

   * 更改MySQL包大小，防止Azkaban连接MySQL阻塞

     sudo vim /etc/my.cnf

     [mysqld]

     max_allowed_packet=1024M

3. 配置Executor Server

   * 编辑azkaban.properties

     ```
     #...
     default.timezone.id=Asia/Shanghai
     #...
     azkaban.webserver.url=http://hadoop102:8081
     
     executor.port=12321
     #...
     database.type=mysql
     mysql.port=3306
     mysql.host=hadoop102
     mysql.database=azkaban
     mysql.user=azkaban
     mysql.password=000000
     mysql.numconnections=100
     executor.metric.reports=true
     executor.metric.milisecinterval.default=60000
     ```

   * 同步azkaban-exec到所有节点

   * 在三台机器上启动executor-server

4. 配置Web Server

   * 编辑azkaban.properties

     ```
     ...
     default.timezone.id=Asia/Shanghai
     ...
     database.type=mysql
     mysql.port=3306
     mysql.host=hadoop102
     mysql.database=azkaban
     mysql.user=azkaban
     mysql.password=000000
     mysql.numconnections=100
     ...
     azkaban.executorselector.filters=StaticRemainingFlowSize,CpuStatus
     ```

   * 修改azkaban-users.xml,添加用户

     ```
     <azkaban-users>
       <user groups="azkaban" password="azkaban" roles="admin" username="azkaban"/>
       <user password="metrics" roles="metrics" username="metrics"/>
       <user password="atguigu" roles="metrics,admin" username="atguigu"/>
     
       <role name="admin" permissions="ADMIN"/>
       <role name="metrics" permissions="METRICS"/>
     </azkaban-users>
     ```

   * 启动web server

   * 访问8081端口

#### 创建MySQL数据库和表

1. 创建gmall_report数据库

2. 创建ads的表

3. sqoop导出脚本hdfs_to_mysql.sh

   ```sql
   #!/bin/bash
   
   hive_db_name=gmall
   mysql_db_name=gmall_report
   
   export_data() {
   /opt/module/sqoop/bin/sqoop export \
   -Dmapreduce.job.queuename=hive \
   --connect "jdbc:mysql://hadoop102:3306/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
   --username root \
   --password 000000 \
   --table $1 \
   --num-mappers 1 \
   --export-dir /warehouse/$hive_db_name/ads/$1 \
   --input-fields-terminated-by "\t" \
   --update-mode allowinsert \
   --update-key $2 \
   --input-null-string '\\N'    \
   --input-null-non-string '\\N'
   }
   
   case $1 in
     "ads_uv_count")
        export_data "ads_uv_count" "dt"
   ;;
     "ads_user_action_convert_day") 
        export_data "ads_user_action_convert_day" "dt"
   ;;
     "ads_user_topic")
        export_data "ads_user_topic" "dt"
   ;;
     "ads_area_topic")
        export_data "ads_area_topic" "dt,iso_code"
   ;;
      "all")
        export_data "ads_user_topic" "dt"
        export_data "ads_area_topic" "dt,iso_code"
        #其余表省略未写
   ;;
   esac
   ```

#### 会员主题指标获取的全调度流程

1. 数据准备

2. 编写Azkaban工作流程配置文件

   * 编写azkaban.project

     azkaban-flow-version: 2.0

   * 编写gmall.flow文件

     ```
     nodes:
       - name: hdfs_to_ods_log
         type: command
         config:
          command: /home/atguigu/bin/hdfs_to_ods_log.sh ${dt}
          
       - name: ods_to_dwd_log
         type: command
         dependsOn: 
          - hdfs_to_ods_log
         config: 
          command: /home/atguigu/bin/ods_to_dwd_log.sh ${dt}
         
       - name: ods_to_dwd_db
         type: command
         dependsOn: 
          - hdfs_to_ods_db
         config: 
          command: /home/atguigu/bin/ods_to_dwd_db.sh all ${dt}
         
       - name: dwd_to_dws
         type: command
         dependsOn:
          - ods_to_dwd_log
          - ods_to_dwd_db
         config:
          command: /home/atguigu/bin/dwd_to_dws.sh ${dt}
         
       - name: dws_to_dwt
         type: command
         dependsOn:
          - dwd_to_dws
         config:
          command: /home/atguigu/bin/dws_to_dwt.sh ${dt}
         
       - name: dwt_to_ads
         type: command
         dependsOn: 
          - dws_to_dwt
         config:
          command: /home/atguigu/bin/dwt_to_ads.sh ${dt}
          
       - name: hdfs_to_mysql
         type: command
         dependsOn:
          - dwt_to_ads
         config:
           command: /home/atguigu/bin/hdfs_to_mysql.sh all
     ```

   * 将两文件下所至一个zip文件

   * 在WebServer新建项目

     * 命名项目
     * 上传zip文件
     * 查看任务流
     * 配置输入dt时间参数