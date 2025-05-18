## 一种代码库中直接写sql就能运行spark程序的框架
## 基本使用
按照如下提交方式，开发直接在resources/com/darrenchan/sqls下面开发sql即可，不用关心scala代码
```shell
export HADOOP_USER_NAME=chenchi; \
export HADOOP_USER_RPCPASSWORD=xxxxx; \
{spark_dir}/spark-submit --master yarn --deploy-mode cluster \
    --queue {queue} \
    --name {task_name}_${grass_region}_{etl_date}_{env} \
    --driver-memory 2G \
    --executor-memory 8G \
    --executor-cores 4 \
    --num-executors 6 \
    --class com.darrenchan.dw.CommonEntry \
    {jar} \
    -e {env} \
    -r {grass_region} \
    -d {etl_date} \
    -tb {table_name} \
    -l {layer}
    
task_name=friends_mart_dim_friends_abtest_user_di
grass_region=ID
etl_date=2024-01-01
env=prod
layer=dim
jar=hdfs://xxx/hdfs/prod/jars/data-warehouse-etl-framework-1.0-SNAPSHOT.jar
```