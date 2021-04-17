# Hadoop支持LZO

#### 环境准备
maven（下载安装，配置环境变量，修改sitting.xml加阿里云镜像）
gcc-c++
zlib-devel
autoconf
automake
libtool
通过yum安装即可，yum -y install gcc-c++ lzo-devel zlib-devel autoconf automake libtool

#### 下载、安装并编译LZO

wget http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz

tar -zxvf lzo-2.10.tar.gz

cd lzo-2.10

./configure -prefix=/usr/local/hadoop/lzo/

make

make install

#### 编译hadoop-lzo源码

1. 下载hadoop-lzo的源码，下载地址：https://github.com/twitter/hadoop-lzo/archive/master.zip
2. 解压之后，修改pom.xml
       <hadoop.current.version>3.1.3</hadoop.current.version>
3. 声明两个临时环境变量
        export C_INCLUDE_PATH=/usr/local/hadoop/lzo/include
        expo rt LIBRARY_PATH=/usr/local/hadoop/lzo/lib 
4. 编译
       进入hadoop-lzo-master，执行maven编译命令
       mvn package -Dmaven.test.skip=true
5. 进入target，hadoop-lzo-0.4.21-SNAPSHOT.jar 即编译成功的hadoop-lzo组件