# 数据生成模块

### 目标数据

收集和分析的数据包括页面数据、事件数据、曝光数据、启动数据和错误数据，使用fastjson、logback

```xml
<dependency>
        <groupId>com.alibaba</groupId>
        <artifactId>fastjson</artifactId>
        <version>1.2.51</version>
    </dependency>

    <!--日志生成框架-->
    <dependency>
        <groupId>ch.qos.logback</groupId>
        <artifactId>logback-core</artifactId>
        <version>${logback.version}</version>
    </dependency>
    <dependency>
        <groupId>ch.qos.logback</groupId>
        <artifactId>logback-classic</artifactId>
        <version>${logback.version}</version>
    </dependency>
```

#### logback使用教程

1. Logback主要用于在磁盘和控制台打印日志

2. 用法：

   * 在resources文件夹下创建logback.xml文件

   * 在logback.xml文件中填写以下配置

     ```xml
     <?xml version="1.0" encoding="UTF-8"?>
     <configuration debug="false">
        <!--定义日志文件的存储地址 勿在 LogBack 的配置中使用相对路径 -->
        <property name="LOG_HOME" value="/tmp/logs/" />
     
        <!-- 控制台输出 -->
        <appender name="STDOUT"
           class="ch.qos.logback.core.ConsoleAppender">
           <encoder
              class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
              <!--格式化输出：%d表示日期，%thread表示线程名，%-5level：级别从左显示5个字符宽度%msg：日志消息，%n是换行符 -->
              <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n</pattern>
           </encoder>
        </appender>
        
        <!-- 按照每天生成日志文件。存储事件日志 -->
        <appender name="FILE"
           class="ch.qos.logback.core.rolling.RollingFileAppender">
           <!-- <File>${LOG_HOME}/app.log</File>设置日志不超过${log.max.size}时的保存路径，注意，如果是web项目会保存到Tomcat的bin目录 下 -->  
           <rollingPolicy
              class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
              <!--日志文件输出的文件名 -->
              <FileNamePattern>${LOG_HOME}/app-%d{yyyy-MM-dd}.log</FileNamePattern>
              <!--日志文件保留天数 -->
              <MaxHistory>30</MaxHistory>
           </rollingPolicy>
           <encoder
              class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
              <pattern>%msg%n</pattern>
           </encoder>
           <!--日志文件最大的大小 -->
           <triggeringPolicy
              class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
              <MaxFileSize>10MB</MaxFileSize>
           </triggeringPolicy>
        </appender>
     
         <!--异步打印日志-->
         <appender name ="ASYNC_FILE" class= "ch.qos.logback.classic.AsyncAppender">
             <!-- 不丢失日志.默认的,如果队列的80%已满,则会丢弃TRACT、DEBUG、INFO级别的日志 -->
             <discardingThreshold >0</discardingThreshold>
             <!-- 更改默认的队列的深度,该值会影响性能.默认值为256 -->
             <queueSize>512</queueSize>
             <!-- 添加附加的appender,最多只能添加一个 -->
             <appender-ref ref = "FILE"/>
         </appender>
     
         <!-- 日志输出级别 -->
        <root level="INFO">
           <appender-ref ref="STDOUT" />
           <appender-ref ref="ASYNC_FILE" />
           <appender-ref ref="error" />
        </root>
     </configuration>
     ```

     

1. 页面数据

   页面数据主要记录一个页面的用户访问情况，包括访问时间、停留时间、页面路径等信息。

2. 事件数据

   事件数据主要记录应用内一个具体操作行为，包括操作类型、操作对象、操作对象描述等信息。

3. 曝光数据

   曝光数据主要记录页面所曝光的内容，包括曝光对象，曝光类型等信息

4. 启动数据

   启动数据记录应用的启动信息

#### 主流埋点方式

1. 代码埋点

   在需要埋点的业务逻辑功能位置调用接口，上报埋点数据。例如，我们对页面中的某个按钮埋点后，当这个按钮被点击时，可以在这个按钮对应的 OnClick 函数里面调用SDK提供的数据发送接口，来发送数据。

2. 可视化埋点

   只需要研发人员集成采集 SDK，不需要写埋点代码，业务人员就可以通过访问分析平台的“圈选”功能，来“圈”出需要对用户行为进行捕捉的控件，并对该事件进行命名。

3. 全埋点

   通过在产品中嵌入SDK，前端自动采集页面上的全部用户行为事件，上报埋点数据，相当于做了一个统一的埋点。然后再通过界面配置哪些数据需要在系统里面进行分析。

#### 埋点数据日志结构

1. 普通页面埋点日志格式

```json
{
  "common": {                  -- 公共信息
    "ar": "230000",              -- 地区编码
    "ba": "iPhone",              -- 手机品牌
    "ch": "Appstore",            -- 渠道
    "md": "iPhone 8",            -- 手机型号
    "mid": "YXfhjAYH6As2z9Iq", -- 设备id
    "os": "iOS 13.2.9",          -- 操作系统
    "uid": "485",                 -- 会员id
    "vc": "v2.1.134"             -- app版本号
  },
"actions": [                     --动作(事件)  
    {
      "action_id": "favor_add",   --动作id
      "item": "3",                   --目标id
      "item_type": "sku_id",       --目标类型
      "ts": 1585744376605           --动作时间戳
    }
  ]，
  "displays": [
    {
      "displayType": "query",        -- 曝光类型
      "item": "3",                     -- 曝光对象id
      "item_type": "sku_id",         -- 曝光对象类型
      "order": 1                        --出现顺序
    },
    {
      "displayType": "promotion",
      "item": "6",
      "item_type": "sku_id",
      "order": 2
    },
    {
      "displayType": "promotion",
      "item": "9",
      "item_type": "sku_id",
      "order": 3
    },
    {
      "displayType": "recommend",
      "item": "6",
      "item_type": "sku_id",
      "order": 4
    },
    {
      "displayType": "query ",
      "item": "6",
      "item_type": "sku_id",
      "order": 5
    }
  ],
  "page": {                       --页面信息
    "during_time": 7648,        -- 持续时间毫秒
    "item": "3",                  -- 目标id
    "item_type": "sku_id",      -- 目标类型
    "last_page_id": "login",    -- 上页类型
    "page_id": "good_detail",   -- 页面ID
    "sourceType": "promotion"   -- 来源类型
  },
"err":{                     --错误
"error_code": "1234",      --错误码
    "msg": "***********"       --错误信息
},
  "ts": 1585744374423  --跳入时间戳
}
```

2. 启动日志格式

```json
{
  "common": {
    "ar": "370000",
    "ba": "Honor",
    "ch": "wandoujia",
    "md": "Honor 20s",
    "mid": "eQF5boERMJFOujcp",
    "os": "Android 11.0",
    "uid": "76",
    "vc": "v2.1.134"
  },
  "start": {   
    "entry": "icon",         --icon手机图标  notice 通知   install 安装后启动
    "loading_time": 18803,  --启动加载时间
    "open_ad_id": 7,        --广告页ID
    "open_ad_ms": 3449,    -- 广告总共播放时间
    "open_ad_skip_ms": 1989   --  用户跳过广告时点
  },
"err":{                     --错误
"error_code": "1234",      --错误码
    "msg": "***********"       --错误信息
},
  "ts": 1585744304000
}
```

#### 服务器准备

1. 更改网卡信息为静态地址

```
# 进入网卡所在目录
cd /etc/sysconfig/network-scripts/
# 备份网卡信息
cp -rp /etc/sysconfig/network-scripts/ifcfg-enp0s3 /etc/sysconfig/network-scripts/ifcfg-enp0s3.bak
# 编辑
vi /etc/sysconfig/network-scripts/ifcfg-enp0s3, 如下
TYPE="Ethernet"
PROXY_METHOD="none"
BROWSER_ONLY="no"
BOOTPROTO="dhcp"
DEFROUTE="yes"
IPV4_FAILURE_FATAL="no"
IPV6INIT="yes"
IPV6_AUTOCONF="yes"
IPV6_DEFROUTE="yes"
IPV6_FAILURE_FATAL="no"
IPV6_ADDR_GEN_MODE="stable-privacy"
NAME="enp0s3"
UUID="ee340a0f-1557-49c7-a927-5069c4e2ab88"
DEVICE="enp0s3"
ONBOOT="yes"
cp -rp /etc/sysconfig/network-scripts/ifcfg-enp0s3 /etc/sysconfig/network-scripts/ifcfg-enp0s8

TYPE="Ethernet"
PROXY_METHOD="none"
BROWSER_ONLY="no"
BOOTPROTO="static"
DEFROUTE="yes"
IPV4_FAILURE_FATAL="no"
IPV6INIT="yes"
IPV6_AUTOCONF="yes"
IPV6_DEFROUTE="yes"
IPV6_FAILURE_FATAL="no"
IPV6_ADDR_GEN_MODE="stable-privacy"
NAME="enp0s8"
UUID="cb7e712d-c2db-4dee-8950-37f9d8a780e9"
DEVICE="enp0s8"
ONBOOT="yes"
IPADDR="172.21.208.200"
NETMASK="255.255.252.0"
GATEWAY="172.21.208.1"
dns1="8.8.8.8"

systemctl restart network
```

2. virtualbox网络设置

增加网卡2，启用网络连接，连接方式为仅主机host-Only网络。

3. 修改主机名和hosts文件
4. 关闭防火墙
5. 配置root

#### 集群分发脚本编写

使用rsync来远程同步

```
#!/bin/bash
#1. 判断参数个数
if [ $# -lt 1 ]
then
  echo Not Enough Arguement!
  exit;
fi
#2. 遍历集群所有机器
for host in hadoop102 hadoop103 hadoop104
do
  echo ====================  $host  ====================
  #3. 遍历所有目录，挨个发送
  for file in $@
  do
    #4 判断文件是否存在
    if [ -e $file ]
    then
      #5. 获取父目录
      pdir=$(cd -P $(dirname $file); pwd)
      #6. 获取当前文件的名称
      fname=$(basename $file)
      ssh $host "mkdir -p $pdir"
      rsync -av $pdir/$fname $host:$pdir
    else
      echo $file does not exists!
    fi
  done
done
```

#### 设置SSH无密登录配置

1. 配置ssh

2. 免密登录原理

   * A服务器ssh-key-gen生成密钥对
   * A服务器把公钥拷贝给B服务器
   * Assh访问B（数据用私钥A加密）
   * B接收到数据后，去授权key中查找A的公钥，并解密数据
   * B采用A公钥加密的数据返回给A

3. 无密钥配置

   * 生成公钥和私钥

     ssh-keygen -t rsa

   * 将公钥拷贝到要免密登录的目标机器上

     ssh-copy-id hadoop102

#### JAVA环境配置

#### 模拟数据生成

#### 集群日志生成脚本 

编写lg.sh脚本

```sh
#!/bin/bash
for i in hadoop102 hadoop103; do
    echo "========== $i =========="
    ssh $i "cd /opt/module/applog/; java -jar gmall2020-mock-log-2020-05-10.jar >/dev/null 2>&1 &"
done
```

