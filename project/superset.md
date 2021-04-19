# 使用Superset进行可视化

#### 安装superset

使用conda安装superset

#### superset部署

1. 安装依赖

   sudo yum install -y python-setuptools

   sudo yum install -y gcc gcc-c++ libffi-devel python-devel python-pip python-wheel openssl-devel cyrus-sasl-devel openldap-devel

2. 安装superset

3. 初始化superset数据库

   superset db upgrade

4. 创建管理员用户

   export FLASK_APP=superset

   flask fab create-admin

5. superset初始化

   superset init

#### 启动Superset

1. 安装gunicorn

2. 启动superset

   gunicorn --workers 5 --timeout 120 --bind hadoop102:8787 "superset.app:create_app()" --daemon 

   **说明：**

   **--workers**：指定进程个数

   **--timeout**：worker进程超时时间，超时会自动重启

   **--bind**：绑定本机地址，即为Superset访问地址

   **--daemon**：后台运行

3. 创建superset启停脚本 superset.sh

   ```sh
   #!/bin/bash
   
   superset_status(){
       result=`ps -ef | awk '/gunicorn/ && !/awk/{print $2}' | wc -l`
       if [[ $result -eq 0 ]]; then
           return 0
       else
           return 1
       fi
   }
   superset_start(){
           # 该段内容取自~/.bashrc，所用是进行conda初始化
           # >>> conda initialize >>>
           # !! Contents within this block are managed by 'conda init' !!
           __conda_setup="$('/opt/module/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
           if [ $? -eq 0 ]; then
               eval "$__conda_setup"
           else
               if [ -f "/opt/module/miniconda3/etc/profile.d/conda.sh" ]; then
                   . "/opt/module/miniconda3/etc/profile.d/conda.sh"
               else
                   export PATH="/opt/module/miniconda3/bin:$PATH"
               fi
           fi
           unset __conda_setup
           # <<< conda initialize <<<
           superset_status >/dev/null 2>&1
           if [[ $? -eq 0 ]]; then
               conda activate superset ; gunicorn --workers 5 --timeout 120 --bind hadoop102:8787 --daemon 'superset.app:create_app()'
           else
               echo "superset正在运行"
           fi
   
   }
   
   superset_stop(){
       superset_status >/dev/null 2>&1
       if [[ $? -eq 0 ]]; then
           echo "superset未在运行"
       else
           ps -ef | awk '/gunicorn/ && !/awk/{print $2}' | xargs kill -9
       fi
   }
   
   
   case $1 in
       start )
           echo "启动Superset"
           superset_start
       ;;
       stop )
           echo "停止Superset"
           superset_stop
       ;;
       restart )
           echo "重启Superset"
           superset_stop
           superset_start
       ;;
       status )
           superset_status >/dev/null 2>&1
           if [[ $? -eq 0 ]]; then
               echo "superset未在运行"
           else
               echo "superset正在运行"
           fi
   esac
   ```

4. 登录Superset

   访问http://hadoop102:8787，并使用管理员账号进行登录

#### Superset使用

1. 对接MySQL数据源
   * 安装依赖 mysqlclient
   * 数据源配置
     * Database配置
     * Table配置
2. 制作仪表盘
   * 创建空白仪表盘
   * 创建图表
   * 编辑仪表盘