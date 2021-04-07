package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;


public class OrderWideApp {

    private static String topicOrderInfo = "dwd_order_info";
    private static String topicOrderDetail = "dwd_detail_info";
    private static String consumerId = "consumer25";

    private static String sinkTopicOrderWide = "dwm_order_wide";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/7  1.获取双流 info detail
        DataStreamSource<String> kafkaInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(topicOrderInfo, consumerId));
        SingleOutputStreamOperator<JSONObject> jsonObjInfoDS = kafkaInfoDS.map(x -> JSON.parseObject(x));

        DataStreamSource<String> kafkaDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(topicOrderDetail, consumerId));
        SingleOutputStreamOperator<JSONObject> jsonObjDetailDS = kafkaDetailDS.map(x -> JSON.parseObject(x));


        // TODO: 2021/4/7 2.填充 String create_date; String create_hour; Long create_ts;
        SingleOutputStreamOperator<OrderInfo> mapInfoDS = jsonObjInfoDS.map(new RichMapFunction<JSONObject, OrderInfo>() {

            SimpleDateFormat dateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderInfo map(JSONObject jsonObject) throws Exception {

                OrderInfo orderInfo = JSON.parseObject(jsonObject.getString("data"), OrderInfo.class);

                String createTime = orderInfo.getCreate_time();

                orderInfo.setCreate_date(createTime.substring(0, 10));
                orderInfo.setCreate_hour(createTime.substring(11, 13));
                orderInfo.setCreate_ts(dateFormat.parse(orderInfo.getCreate_time()).getTime());

                return orderInfo;
            }
        });


        SingleOutputStreamOperator<OrderDetail> mapDetailDS = jsonObjDetailDS.map(new RichMapFunction<JSONObject, OrderDetail>() {

            SimpleDateFormat dateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderDetail map(JSONObject jsonObject) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonObject.getString("data"), OrderDetail.class);

                orderDetail.setCreate_ts(dateFormat.parse(orderDetail.getCreate_time()).getTime());

                return orderDetail;
            }
        });


        // TODO: 2021/4/7 3.设置事件时间和水位线
        SingleOutputStreamOperator<OrderInfo> markinfoDS = mapInfoDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderInfo>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(OrderInfo orderInfo) {
                return orderInfo.getCreate_ts();
            }
        });

        SingleOutputStreamOperator<OrderDetail> markDetailDS = mapDetailDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(OrderDetail orderDetail) {
                return orderDetail.getCreate_ts();
            }
        });


        // TODO: 2021/4/7 4.双流join
        //通过orderId关联
        KeyedStream<OrderInfo, Long> keyedInfoDS = markinfoDS.keyBy(x -> x.getId());
        KeyedStream<OrderDetail, Long> keyedDetailDS = markDetailDS.keyBy(x -> x.getOrder_id());

        SingleOutputStreamOperator<OrderWide> joinDS = keyedInfoDS.intervalJoin(keyedDetailDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

//        joinDS.print("join");

        // TODO: 2021/4/7 5.关联维度
        SingleOutputStreamOperator<OrderWide> withDS = joinDS.map(new RichMapFunction<OrderWide, OrderWide>() {

            Connection connection = null;
            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = PhoenixUtil.getConn();
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderWide map(OrderWide orderWide) throws Exception {

                //地区表    province_name;  province_area_code;   province_iso_code;  province_3166_2_code;
                Long provinceId = orderWide.getProvince_id();
                JSONObject provinceJsonObj = PhoenixUtil.queryOne("select * from GMALL2020_PROVINCE_INFO where ID = '" + provinceId + "'", connection);
                orderWide.setProvince_name(provinceJsonObj.getString("NAME"));
                orderWide.setProvince_area_code(provinceJsonObj.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(provinceJsonObj.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(provinceJsonObj.getString("ISO_3166_2"));

                //sku表    Long spu_id;   Long tm_id;  Long category3_id;  String spu_name;  String tm_name;  String category3_name;
                Long skuId = orderWide.getSku_id();
                JSONObject skuJsonObj = PhoenixUtil.queryOne("select * from GMALL2020_SKU_INFO where ID = '" + skuId + "'", connection);
                orderWide.setSpu_id(skuJsonObj.getLong("SPU_ID"));
                orderWide.setSpu_name(skuJsonObj.getString("SPU_NAME"));
                orderWide.setTm_id(skuJsonObj.getLong("TM_ID"));
                orderWide.setTm_name(skuJsonObj.getString("TM_NAME"));
                orderWide.setCategory3_id(skuJsonObj.getLong("CATEGORY3_ID"));
                orderWide.setCategory3_name(skuJsonObj.getString("CATEGORY3_NAME"));

                //用户表
                Long userId = orderWide.getUser_id();
                JSONObject userJsonObj = PhoenixUtil.queryOne("select * from GMALL2020_USER_INFO where ID = '" + userId + "'", connection);
                orderWide.setUser_gender(userJsonObj.getString("GENDER_NAME"));

                long currTs = new Date().getTime();
                long userTs = simpleDateFormat.parse(userJsonObj.getString("BIRTHDAY")).getTime();
                long userAge = (currTs - userTs) / (365 * 24 * 60 * 60 * 1000L);
                orderWide.setUser_age((int)userAge);

                return orderWide;

            }
        });


        //scala里的写法
//        withDS.map(x -> JSON.toJSONString(x,new SerializeConfig(true))).print("scala");

        withDS.map(x -> JSON.toJSONString(x)).addSink(MyKafkaUtil.getKafkaSink(sinkTopicOrderWide));

        withDS.map(x -> JSON.toJSONString(x)).print("java");

        env.execute();

    }

}
