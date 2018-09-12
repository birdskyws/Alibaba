package org.aura.bigdata1503;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


import com.google.common.collect.Sets;
import kafka.serializer.StringDecoder;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

/**
 * 每个商家实时交易次数，并存入redis，其中key 为”jiaoyi+<shop_id>”, value 为累计的次数
 * 每个城市发生的交易次数，并存储redis，其中key为“交易+<城市名 称>”,value 为累计的次数
 */
public class Streaming {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://mysql1:3306/Alibaba";
    static final String USER = "shell";
    static final String PASS = "123456";
    static final Jedis jedis = new Jedis("datanode2");

    static JavaStreamingContext jssc;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("KafkaStream");
        conf.set("spark.streaming.stopGracefullyOnShutdown","true");
        jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        stream();
        jssc.start();
        jssc.awaitTermination();
    }
    /**
     * 工具类，更新redis
     */
    public static void setJedisValue(String key,String  value){
        if(jedis.exists(key))
        {
            String old_times = jedis.get(key);
            int new_times = Integer.parseInt(value)+Integer.parseInt(value);
            jedis.set(key,new Integer(new_times).toString());
        }
        else{
            jedis.set(key,value);
        }
    }

    /***
     * kafka setting
     * @return
     */
    public static Map<String,String> getKafkaConfig(){
        Map<String,String> kafkaParams =  new HashMap<String,String>();
        kafkaParams.put("bootstrap.servers","hbase:9092");
        kafkaParams.put("auto.offset.reset","smallest");
        kafkaParams.put("group.id","homework");
        return kafkaParams;
    }
    /**
     * 编写 spark streaming 程序，依次读取 kafka 中 user_pay 主题数据，并统计:
     * 每个商家实时交易次数，并存入redis，其中key 为”jiaoyi+<shop_id>”, value 为累计的次数
     * 每个城市发生的交易次数，并存储redis，其中key为“交易+<城市名 称>”,value 为累计的次数
     */
    public static void stream()
    {

        JavaPairInputDStream<String,String> inputDStream = KafkaUtils.createDirectStream(
                jssc,String.class,String.class,StringDecoder.class,StringDecoder.class,
                getKafkaConfig(),Sets.newHashSet("user_pay"));

        inputDStream.cache();
        transaction(inputDStream);
        cityCount(inputDStream);
    }
    /**
     * 统计城市交易次数
     * kafka data:user_id为 key，shop_id+”,”+time_stamp 为 value
     * shopMap存储<shop_id,city_name>
     */
    public static void cityCount(JavaPairInputDStream<String,String> inputDStream){

        Map<String,String> map = shop();
        Broadcast<Map<String,String>> shopMapBroadcast = jssc.sparkContext().broadcast(map);
        inputDStream.foreachRDD(rdd->{
            /**
             * map转化
             * <city_name,1>
             */
            JavaPairRDD<String,Integer> citynameRDD = rdd.mapToPair(v->{
                String shop_id = v._2.split(",")[0];
                String city_name =shopMapBroadcast.getValue().get(shop_id);
                return new Tuple2<>(city_name,1);
            });
            /**
             * 按 city_name 聚合
             */
            citynameRDD.reduceByKey((v1, v2) -> v1+v2)
                    .foreach(info->{
                        setJedisValue(info._1,info._2.toString());
                    });
        });
    }
    /**
     * kafka data:user_id为 key，shop_id+”,”+time_stamp 为 value
     * 保存shop_id计数到redis中。
     * param:inputDStream KafkaDStream
     */
    public static void transaction(JavaPairInputDStream<String,String> inputDStream)
    {
        inputDStream.foreachRDD(rdd->{
            /**
             * map <shop_id,1>
             * 聚合 shop_id
             */
            rdd.mapToPair(l->{
                String[] arr = l._2.split(",");
                return new Tuple2<>(arr[0],1);
            }).reduceByKey((v1,v2)->v1+v2)
            .foreach(info->{
                setJedisValue(info._1,info._2.toString());
            });
        });
    }
    /**
     * 返回shop_id 和 city_name 对应的Map
     */
    public static Map<String,String> shop()
    {
        Connection conn = null;
        Statement stmt = null;
        Map<String,String> map = new HashMap<>();
        try{
            // 注册 JDBC 驱动
            Class.forName("com.mysql.jdbc.Driver");

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT shop_id, city_name, per_pay FROM shop_info";
            ResultSet rs = stmt.executeQuery(sql);

            // 展开结果集数据库
            while(rs.next()){
                // 通过字段检索
                Long id  = rs.getLong("shop_id");
                String name = rs.getString("city_name");
                String key = id.toString();
                String value = name;
                map.put(key, value);
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
        return map;
    }
    public static void jedisTest(){
        //添加
        Jedis jedis = new Jedis("datanode2");
        jedis.set("wsn","1");
        jedis.set("wsn","2");
        //读取
        String a  = jedis.get("wsn");
        System.out.println(a);
        //添加2
        //读取
        String c = jedis.get("wsn2");
        System.out.println(c);
        System.out.println(jedis.exists("wsn2"));
    }
}

