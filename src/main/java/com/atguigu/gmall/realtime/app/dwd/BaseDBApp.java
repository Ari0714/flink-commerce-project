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

    private static String topic = "";
    private static String consumerId = "consumer01";

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

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(x -> JSON.parseObject(x));

        OutputTag<String> orderDetailTag = new OutputTag<String>("order_detail"){};

        SingleOutputStreamOperator<String> splitDS = jsonObjDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                String table = jsonObject.getString("table");
                if (table.equals("order_info"))
                    collector.collect(jsonObject.toJSONString());
                else if (table.equals("order_detail"))
                    context.output(orderDetailTag, jsonObject.toString());
            }
        });

        splitDS.addSink(MyKafkaUtil.getKafkaSink("dwd_order_info"));
        splitDS.getSideOutput(orderDetailTag).addSink(MyKafkaUtil.getKafkaSink("dwd_detail_info"));


        env.execute();
    }

}
