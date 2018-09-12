package org.aura.bigdata1503;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.aura.bigdata1503.entity.Pay;
import org.aura.bigdata1503.entity.Shop;
import org.aura.bigdata1503.entity.View;


/**
 * 第四大题
 */
public class SparkSqlForQ4 {
    private static String shop_info = "hdfs://namenode:9000/Hive/shop_info";
    private static String user_pay = "hdfs://namenode:9000/Hive/user_pay";
    private static String user_view = "hdfs://namenode:9000/Hive/user_view";
    public static void main(String[] args) {
        System.out.println("光环毕业大作业 阿里巴巴真实消费数据大数据开发实验教程");
        SparkSession spark = SparkSession.builder().appName("homework2").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        //milktea(spark);
        //day(spark);
        topView(spark);
        spark.close();
    }

    /**
     * 输出北京、上海、广州和深圳四个城市最受欢迎的 5 家奶茶商店和中式快餐编号
     * 最受欢迎是指以下得分最高:0.7✖(平均评分/5)+0.3✖(平均消费金额/最高消费金额)，
     * 注:最高消费金额和平均消费金额是从所有消费记录统计出来的
     *
     * 时间：2018.9.12
     * by birdskyws@163.com
     *
     * @param spark
     */
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
        System.out.println("最高消费金额："+max_pay);

        spark.sql("select distinct(cate_2_name) from shop_info")
                .javaRDD()
                .collect()
                .forEach(l->{
             System.out.print(l);
        });
        /**
         * 输出北京、上海、广州和深圳四个城市最受欢迎的 5 家奶茶商
         * row_number() over (partition by city_name order by '评分' desc) as judge_score
         * judge_scoure<=5 选出5家
         */
        String sql = "select shop_id,t.judge_score,t.city_name,t.cate_3_name from" +
                "(select * ,row_number() over (partition by city_name " +
                "order by pre_pay/20*0.3+score/5*0.7 desc) as judge_score from shop_info "+
                "where cate_3_name='奶茶' and city_name in('北京','上海','广州','深圳')) t " +
                "where t.judge_score<=5  order by t.city_name,judge_score";
        // 查看sql
        // System.out.println(sql);
        spark.sql(sql).show();
        /**
         * 输出北京、上海、广州和深圳四个城市最受欢迎的 5 家中餐
         */
        String sql2 = "select shop_id,t.judge_score,t.city_name,t.cate_2_name from" +
                "(select * ,row_number() over (partition by city_name " +
                "order by pre_pay/20*0.3+score/5*0.7 desc) as judge_score from shop_info "+
                "where cate_2_name='中餐' and city_name in('北京','上海','广州','深圳')) t " +
                "where t.judge_score<=5  order by t.city_name,judge_score";
        spark.sql(sql2).show();

    }

    /**
     * 输出该商店每天、每周和每月的被浏览数量
     * @param spark
     */
    public static void day(SparkSession spark){
        JavaRDD<View> viewRDD = spark.read().textFile(user_view)
                .javaRDD().map(line -> {
                    String[] arr = line.split(",");
                    return new View(Long.parseLong(arr[0]),Long.parseLong(arr[1]),arr[2]);
                });

        Dataset<Row> viewDF = spark.createDataFrame(viewRDD, View.class);

        viewDF.createOrReplaceTempView("user_view");
        viewDF.printSchema();
        /**
         * 2层Select
         * 里层:转换ts,日、周、月
         * 外层:按day,week,month聚合
         */
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

    /**
     * 找到被浏览次数最多的 50 个商家，并输出他们的城市以及人均消费，并选择合适
     * 的图表对结果进行可视化
     * @param spark
     */
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
        /**
         * 2层select
         * 里层，按shop_id聚合
         * 外层，查询city_name,pre_pay
         */
        spark.sql("select city_name,pre_pay,v.view from shop_info join" +
                "(select shop_id,count(*) as view from user_view group by shop_id) v " +
                "on shop_info.shop_id=v.shop_id " +
                "order by v.view desc limit 50 " ).show(50);
    }

}
