package org.aura.bigdata1503;

package org.aura.bigdata1503.entity;


public class Presto {
    /**
     * 以城市为单位，统计每个城市总体消费金额
     */
    public static String city_agg = "select city_name,sum(pre_pay) from user_pay join shop_info on user_pay.shop_id=shop_info.shop_id group by city_name;";
    /**
     * 以天为单位，统计所有商家交易发生次数和被用户浏览次数 (曲线图)
     */
    public static String day_shop_view = "select day,count(1) from (select format_datetime(from_unixtime(to_unixtime(cast (ts as timestamp))),'yyyy-MM-dd') as day ,* from user_pay t) group by day";
    public static String day_shop_pay = "select day,count(1) from (select format_datetime(from_unixtime(to_unixtime(cast (ts as timestamp))),'yyyy-MM-dd') as day ,* from user_view t) group by day";
    /**
     * 统计最受欢迎的前 10 类商品(按照二级分类统计)
     */
    public static String top_cate2name = "select cate_2_name,c1*0.3+c2*0.7 as score from (select t1.cate_2_name,t1.c1,t2.c2 from (select cate_2_name,count(1) as c1 from user_pay join shop_info on user_pay.shop_id=shop_info.shop_id group by cate_2_name) t1 join (select cate_2_name,count(1) as c2 from user_view join shop_info on user_view.shop_id=shop_info.shop_id group by cate_2_name) t2 on t1.cate_2_name=t2.cate_2_name) order by score desc";
    public static String top_pay = "";
}
/*
select day,count(1) from (select format_datetime(from_unixtime(to_unixtime(cast (ts as timestamp))),'yyyy-MM-dd') as day ,* from user_pay t) group by day;
select day,count(1) from (select format_datetime(from_unixtime(to_unixtime(cast (ts as timestamp))),'yyyy-MM-dd') as day ,* from user_view t) group by day;
select t1.cate_2_name,t1.c1,t2.c2 from (select cate_2_name,count(1) as c1 from user_pay join shop_info on user_pay.shop_id=shop_info.shop_id group by cate_2_name) t1 join (select cate_2_name,count(1) as c2 from user_view join shop_info on user_view.shop_id=shop_info.shop_id group by cate_2_name) t2 on t1.cate_2_name=t2.cate_2_name;
select cate_2_name,c1*0.3+c2*0.7 as score from (select t1.cate_2_name,t1.c1,t2.c2 from (select cate_2_name,count(1) as c1 from user_pay join shop_info on user_pay.shop_id=shop_info.shop_id group by cate_2_name) t1 join (select cate_2_name,count(1) as c2 from user_view join shop_info on user_view.shop_id=shop_info.shop_id group by cate_2_name) t2 on t1.cate_2_name=t2.cate_2_name) order by score desc;
*/
/*
        cate_2_name     |       score
        --------------------+--------------------
        快餐               |  9582045.799999999
        超市               |  6487143.499999999
        便利店             | 1797257.7999999998
        休闲茶饮           |          1573034.9
        休闲食品           |          1220338.5
        烘焙糕点           | 1166330.0999999999
        小吃               |          1126353.5
        中餐               |  666750.2999999999
        火锅               |  552967.7999999999
        其他美食           |           447264.1
        烧烤               |            82123.1
        汤/粥/煲/砂锅/炖菜 | 61677.399999999994
        网吧网咖           |  8913.099999999999
        个人护理           |  5840.599999999999
        美容美发           |  5206.299999999999
        本地购物           |             4644.2
 */