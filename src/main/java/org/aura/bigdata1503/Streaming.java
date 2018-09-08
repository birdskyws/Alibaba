package org.aura.bigdata1503;
import java.io.BufferedReader;
import java.sql.*;


import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.*;

public class Streaming {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://mysql1:3306/Alibaba";

    // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "shell";
    static final String PASS = "123456";
    public static void main(String[] args) {
        //shop();
        readUser_pay();
        //kafka_producer();
    }
    public static void kafka_producer()
    {
        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            System.out.println("i:" + i);
            long startTime = System.currentTimeMillis();
            producer.send(new ProducerRecord<String, String>("exam2",
                            Integer.toString(i), Integer.toString(i))
                    ,new DemoCallBack(startTime, Integer.toString(i), Integer.toString(i)));
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
    // config
    public static Properties getConfig()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hbase:9092,datanode2:9092,datanode3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static void readUser_pay(){
        StringBuffer buffer = new StringBuffer();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;

        Configuration conf = new Configuration();
        String txtFilePath = "hdfs://namenode:9000/Hive/user_pay/user_pay.txt";

        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        try
        {
            FileSystem fs = FileSystem.get(URI.create(txtFilePath),conf);
            fsr = fs.open(new Path(txtFilePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            while ((lineTxt = bufferedReader.readLine()) != null)
            {
                String[] arr = lineTxt.split(",");
                String key = arr[0];
                String value = arr[1]+arr[2];
                long startTime = System.currentTimeMillis();
                producer.send(new ProducerRecord<String, String>("user_pay",key,value)
                        ,new DemoCallBack(startTime, key, value));
                Thread.sleep(10);
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            if (bufferedReader != null)
            {
                try
                {
                    bufferedReader.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void shop()
    {
        Connection conn = null;
        Statement stmt = null;
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
                int pre_pay = rs.getInt("per_pay");

                // 输出数据
                System.out.print("ID: " + id);
                System.out.print(", 城市名称: " + name);
                System.out.print(", 平均消费: " + pre_pay);
                System.out.print("\n");
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
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final String key;
    private final String message;

    public DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}