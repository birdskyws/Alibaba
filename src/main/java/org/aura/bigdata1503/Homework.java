package org.aura.bigdata1503;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.aura.bigdata1503.entity.Pay;
import org.aura.bigdata1503.entity.Shop;
import org.aura.bigdata1503.entity.View;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;


public class Homework {
    private static String shop_info = "hdfs://namenode:9000/Hive/shop_info";
    private static String user_pay = "hdfs://namenode:9000/Hive/user_pay";
    private static String user_view = "hdfs://namenode:9000/Hive/user_view";
    public static void main(String[] args) {
        System.out.println("光环毕业大作业 阿里巴巴真实消费数据大数据开发实验教程");
        SparkSession spark = SparkSession.builder().appName("homework2").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        //top(spark);
        //milktea(spark);
        //day(spark);
        topView(spark);
        spark.close();
    }
    public static void milktea(SparkSession spark)
    {
        JavaRDD<Shop> shopRDD = spark.read().textFile(shop_info)
                .javaRDD().map(line -> {
                    String[] arr = line.split(",");
                    if(arr.length ==10)
                        return new Shop(Long.parseLong(arr[0]),
                            arr[1], arr[2], arr[3], arr[4],arr[5], arr[6], arr[7], arr[8],arr[9]);
                    else
                        return new Shop(Long.parseLong(arr[0]),
                                arr[1], arr[2], arr[3], arr[4],arr[5], arr[6], arr[7], arr[8],"未知");
                });

        Dataset<Row> shopDF = spark.createDataFrame(shopRDD, Shop.class);

        shopDF.persist();
        shopDF.createOrReplaceTempView("shop_info");
        int max_pay = spark.sql("select max(pre_pay) from shop_info").first().getInt(0);

         spark.sql("select distinct(cate_2_name) from shop_info").javaRDD().collect().forEach(l->{
             System.out.print(l);
         });

        String sql = "select shop_id,t.judge_score,t.city_name,t.cate_3_name from" +
                "(select * ,row_number() over (partition by city_name " +
                "order by pre_pay/20*0.3+score/5*0.7 desc) as judge_score from shop_info "+
                "where cate_3_name='奶茶' and city_name in('北京','上海','广州','深圳')) t " +
                "where t.judge_score<=5  order by t.city_name,judge_score";
        System.out.println(sql);
        spark.sql(sql).show();

        String sql2 = "select shop_id,t.judge_score,t.city_name,t.cate_2_name from" +
                "(select * ,row_number() over (partition by city_name " +
                "order by pre_pay/20*0.3+score/5*0.7 desc) as judge_score from shop_info "+
                "where cate_2_name='中餐' and city_name in('北京','上海','广州','深圳')) t " +
                "where t.judge_score<=5  order by t.city_name,judge_score";
        spark.sql(sql2).show();

    }
    public static void topView(SparkSession spark)
    {
        JavaRDD<Shop> shopRDD = spark.read().textFile(shop_info)
                .javaRDD().map(line -> {
                    String[] arr = line.split(",");
                    if(arr.length ==10)
                        return new Shop(Long.parseLong(arr[0]),
                                arr[1], arr[2], arr[3], arr[4],arr[5], arr[6], arr[7], arr[8],arr[9]);
                    else
                        return new Shop(Long.parseLong(arr[0]),
                                arr[1], arr[2], arr[3], arr[4],arr[5], arr[6], arr[7], arr[8],"未知");
                });

        Dataset<Row> shopDF = spark.createDataFrame(shopRDD, Shop.class);

        shopDF.persist();
        shopDF.createOrReplaceTempView("shop_info");

        JavaRDD<View> viewRDD = spark.read().textFile(user_view)
                .javaRDD().map(line -> {
                    String[] arr = line.split(",");
                    return new View(Long.parseLong(arr[0]),Long.parseLong(arr[1]),arr[2]);
                });
        Dataset<Row> viewDF = spark.createDataFrame(viewRDD, View.class);

        viewDF.createOrReplaceTempView("user_view");


        spark.sql("select city_name,pre_pay,v.view from shop_info join" +
                "(select shop_id,count(*) as view from user_view group by shop_id) v " +
                "on shop_info.shop_id=v.shop_id " +
                "order by v.view desc limit 50 " ).show(50);

    }
    public static void day(SparkSession spark){
        JavaRDD<View> viewRDD = spark.read().textFile(user_view)
                .javaRDD().map(line -> {
                    String[] arr = line.split(",");
                    return new View(Long.parseLong(arr[0]),Long.parseLong(arr[1]),arr[2]);
                });

        Dataset<Row> viewDF = spark.createDataFrame(viewRDD, View.class);

        viewDF.createOrReplaceTempView("user_view");
        viewDF.printSchema();
        String day_sql = "select shop_id,day,count(*) as total from" +
                "(select *,from_unixtime(unix_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as day " +
                "from user_view) t where t.shop_id='1197' " +
                "group by day,shop_id order by day desc";
        spark.sql(day_sql).show();

        String week_sql = "select shop_id,week,count(*) as total from" +
                "(select *,weekofyear(ts) as week " +
                "from user_view) t where t.shop_id='1197' " +
                "group by week,shop_id order by week desc";
        spark.sql(week_sql).show();


        String month_sql = "select shop_id,month,count(*) as total from" +
                "(select *,from_unixtime(unix_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM') as month " +
                "from user_view) t where t.shop_id='1197' " +
                "group by month,shop_id order by month desc";
        spark.sql(month_sql).show();

    }
    public static void top(SparkSession spark) {


        JavaRDD<Shop> shopRDD = spark.read().textFile(shop_info)
                .javaRDD().map(line -> {
                    String[] arr = line.split(",");
                    return new Shop(Long.parseLong(arr[0]),
                            arr[1], arr[2], arr[3], arr[4],arr[5], arr[6], arr[7], arr[8],arr[9]);
                });
        Dataset<Row> shopDF = spark.createDataFrame(shopRDD, Shop.class);
        shopDF.printSchema();
        shopDF.createOrReplaceTempView("shop");


        JavaRDD<Pay> payRDD = spark.read().textFile(user_pay)
                .javaRDD().map(line->{
                    String[] arr = line.split(",");
                    return new Pay(Long.parseLong(arr[0]),Long.parseLong(arr[1]));
                });
        Dataset<Row> payDF = spark.createDataFrame(payRDD,Pay.class);
        payDF.printSchema();
        payDF.createOrReplaceTempView("pay");

        spark.sql("select shop_id,count(1) as num from pay group by shop_id")
                .createOrReplaceTempView("pay_count");

        spark.sql("select shop.shop_id,shop.pre_pay*pay_count.num as total from shop " +
                "join pay_count on shop.shop_id=pay_count.shop_id order by total desc limit 10")
                .show();
        /*
+-------+--------+
|shop_id|   total|
+-------+--------+
|   1629|13609280|
|     58| 9047971|
|    517| 6463128|
|   1928| 6273120|
|   1821| 5465635|
|    934| 4541058|
|   1296| 4261054|
|   1302| 4197347|
|   1535| 4018443|
|    580| 3556344|
+-------+--------+
*/
    }
}
