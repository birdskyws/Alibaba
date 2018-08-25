## 创建工程文件夹，建立git项目，测试协同开发。

## 编写README.md 描述项目

## 开发环境检查，mysql,hdfs,presto,yarn,redis。每人配置自己环境。
- mysql
- hdfs
- presto
- yarn
- redis
- kafka
- spark

## 数据目录:
    /Alibaba/data/shop_info.txt
    /Alibaba/data/user_pay.txt
    /Alibaba/data/user_view.txt
## 项目分解：  
### 任务一  
#### 1.shop_info导入到商家数据表。     
Mysql信息 db：Alibaba，table：shop_info。 
字段名按文档：    
    ```
    shop_id(int 10),city_name(varchar 100),location_id(varchar 10),per_pay(int 10),score(int 10),comment_cnt(int 10),shop_level(int 10),cate_1_name(varchar 50),cate_2_name(varchar 50),cate3_2_name(varchar 50)
    ```   
#### 2.user_pay,user_view 导入HDFS，创建Alibaba目录。     
URL：hdfs://bigdata:9000/Alibaba/user_pay.txt
## 任务二
sqoop 导入Hdfs，依照之前规范。
## 任务三
Presto 分析
- 以城市为单位，统计每个城市总体消费金额 (饼状图)
- 以天为单位，统计所有商家交易发生次数和被用户浏览次数 (曲线图)
## 任务四
利用Spark RDD或Spark DataFrame分析产生以下结果
- 平均日交易额最大的前 10 个商家，并输出他们各自的交易额
- 分别输出北京、上海和广州三个城市最受欢迎的 10 家火锅商店编号
## 任务五
流式分析，利用 spark streaming 分析所有商家实时交易发生次数次数
- 编写Kafka produer程序，将user_pay数据依次写入kafka中的user_pay主题，每条数据写入间隔为 10 毫秒。
- 编写 spark streaming 程序，依次读取 kafka 中 user_pay 主题数据
## 进度安排
| 任务 | 时间安排 | 人员|
| ---| ------:| :---------:|
| 任务一 | 8.25 - 8.26 | 全体 | 
| 任务二 | 8.26 - 8.27 | 全体 |
| 任务三 | 8.28 - 9.2| 全体 |
| 任务四 | 9.1 - 9.5 | 全体 |
| 任务五 | 9.3 - 9.7 | 全体 |
| 总结文档 | 9.5 - 9.12 | 全体 |


## 附录版本管理：
尽量使用Aura的pom文件   

| 名称 |  版本 | api |
| --------   | -----:   | :----: |
|Mysql| 5.6| |
|HDFS| 2.7.3 | |
|presto| | |
|yarn| | |
|redis| 3.2.8 | |
|kafka| 2.10 | |
|spark| 2.1.1 | |
