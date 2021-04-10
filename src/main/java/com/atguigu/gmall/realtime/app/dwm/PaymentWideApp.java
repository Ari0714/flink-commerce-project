package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.RichMapFunction;
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

public class PaymentWideApp {

    private static String topicPaymentInfo = "dwd_payment_info";
    private static String topicOrderWide = "dwm_order_wide";
    private static String consumerId = "consumer41";

    private static String sinkTopic = "dwm_payment_wide";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/7  1.获取双流 pay order_wide
        DataStreamSource<String> kafkaPaymentInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(topicPaymentInfo, consumerId));
        SingleOutputStreamOperator<PaymentInfo> PaymentInfoDS = kafkaPaymentInfoDS.map(x -> JSON.parseObject(x).getJSONObject("data"))
                .map(x -> JSON.parseObject(x.toString(), PaymentInfo.class));

        DataStreamSource<String> kafkaOrderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(topicOrderWide, consumerId));
        SingleOutputStreamOperator<OrderWide> OrderWideDS = kafkaOrderWideDS.map(x -> JSON.parseObject(x))
                .map(x -> JSON.parseObject(x.toString(), OrderWide.class));


        // TODO: 2021/4/7 2.设置watermark
        SingleOutputStreamOperator<PaymentInfo> markPaymentDS = PaymentInfoDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<PaymentInfo>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(PaymentInfo paymentInfo) {
                //统一时间处理方式
                return DateTimeUtil.toTs(paymentInfo.getCallback_time());
            }
        });

        SingleOutputStreamOperator<OrderWide> markOrderWideDS = OrderWideDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderWide>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(OrderWide orderWide) {
                return DateTimeUtil.toTs(orderWide.getCreate_time());
            }
        });


        // TODO: 2021/4/7 3.分区 inteval join
        KeyedStream<PaymentInfo, Long> keyedPaymentDS = markPaymentDS.keyBy(x -> x.getOrder_id());
        KeyedStream<OrderWide, Long> keyedOrderWideDS = markOrderWideDS.keyBy(x -> x.getOrder_id());

//        keyedOrderWideDS.print("pay");
//        keyedPaymentDS.print("wide");

        SingleOutputStreamOperator<PaymentWide> joinDS = keyedPaymentDS.intervalJoin(keyedOrderWideDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo,orderWide));
                    }
                });


//        joinDS.print("join");


        //类转json串
        joinDS.map(x -> JSON.toJSONString(x)).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
