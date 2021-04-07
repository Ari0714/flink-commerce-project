package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseDBApp {

    private static String topic = "gmall-2020-04_flink";
    private static String consumerId = "consumer03";
    private static String targetDatabase = "gmall1122";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/6 1.maxwell数据来源一个主题
        SingleOutputStreamOperator<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, consumerId))
                .filter(x -> x.length() > 10);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(x -> JSON.parseObject(x))
                .filter(x -> x.getJSONObject("data").size() > 5);

        OutputTag<String> orderDetailTag = new OutputTag<String>("orderDetailTag"){};
        OutputTag<String> baseProvinceTag = new OutputTag<String>("baseProvinceTag"){};
        OutputTag<String> paymentInfoTag = new OutputTag<String>("paymentInfoTag"){};
        OutputTag<String> favorInfoTag = new OutputTag<String>("favorInfoTag"){};
        OutputTag<String> cartInfoTag = new OutputTag<String>("cartInfoTag"){};
        OutputTag<String> commentInfoTag = new OutputTag<String>("commentInfoTag"){};

        SingleOutputStreamOperator<String> splitDS = jsonObjDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                String table = jsonObject.getString("table");
                String currentDatabase = jsonObject.getString("database");
                if (targetDatabase.equals(currentDatabase)){
                    if (table.equals("order_info"))
                        collector.collect(jsonObject.toJSONString());
                    else if (table.equals("order_detail"))
                        context.output(orderDetailTag, jsonObject.toString());
                    else if (table.equals("base_province"))
                        context.output(baseProvinceTag, jsonObject.toString());
                    else if (table.equals("payment_info"))
                        context.output(paymentInfoTag, jsonObject.toString());
                    else if (table.equals("favor_info"))
                        context.output(favorInfoTag, jsonObject.toString());
                    else if (table.equals("cart_info"))
                        context.output(cartInfoTag, jsonObject.toString());
                    else if (table.equals("comment_info"))
                        context.output(commentInfoTag, jsonObject.toString());
                }
            }
        });

        splitDS.addSink(MyKafkaUtil.getKafkaSink("dwd_order_info"));
        splitDS.getSideOutput(orderDetailTag).addSink(MyKafkaUtil.getKafkaSink("dwd_detail_info"));
        splitDS.getSideOutput(baseProvinceTag).addSink(MyKafkaUtil.getKafkaSink("dwd_base_province"));
        splitDS.getSideOutput(paymentInfoTag).addSink(MyKafkaUtil.getKafkaSink("dwd_payment_info"));
        splitDS.getSideOutput(favorInfoTag).addSink(MyKafkaUtil.getKafkaSink("dwd_favor_info"));
        splitDS.getSideOutput(cartInfoTag).addSink(MyKafkaUtil.getKafkaSink("dwd_cart_info"));
        splitDS.getSideOutput(commentInfoTag).addSink(MyKafkaUtil.getKafkaSink("dwd_comment_info"));


        env.execute();
    }

}
