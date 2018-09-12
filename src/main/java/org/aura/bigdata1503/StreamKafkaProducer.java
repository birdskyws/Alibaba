package org.aura.bigdata1503;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

/**
 * 编写 Kafka produer 程序，将 user_pay 数据依次写入 kafka 中的 user_pay 主题
 * 中，每条数据写入间隔为 10 毫秒，其中 user_id 为 key，
 * shop_id+”,”+time_stamp 为 value
 */
public class StreamKafkaProducer {
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

        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;

        Configuration conf = new Configuration();
        String txtFilePath = "hdfs://namenode:9000/Hive/user_pay/user_pay.txt";

        Properties props = getConfig();
        Producer<String, String> producer;
        producer = new KafkaProducer<String, String>(props);

        try
        {
            FileSystem fs = FileSystem.get(URI.create(txtFilePath),conf);
            fsr = fs.open(new Path(txtFilePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            /**
             * readLine 按行读取hdfs
             * Thead.sleep(10)每10ms
             * ProducerRecord<>(topic,key,value)
             * producer.send(ProducerRecord)
             */
            while ((lineTxt = bufferedReader.readLine()) != null)
            {
                String[] arr = lineTxt.split(",");
                String key = arr[0];
                String value = arr[1]+","+arr[2];
                long startTime = System.currentTimeMillis();
                producer.send(new ProducerRecord<>("user_pay",key,value)
                        ,new DemoCallBack(startTime, key, value));
                //间隔10ms
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

    public static void main(String[] args) {
        readUser_pay();
    }
}
/* 通过回调方式显示写入kafka的数据
 */
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