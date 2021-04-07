package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class JumpApp {

    private static String topic = "dwd_page_log";
    private static String consumerId = "consumer11";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/7  这个需求一定要设置指定事件时间
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, consumerId));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(x -> JSON.parseObject(x))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject) {
                        return jsonObject.getLong("ts");
                    }
                });

        Pattern<JSONObject, JSONObject> targetPattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null || jsonObject.getJSONObject("page").getString("last_page_id").length() == 0;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("page_id") != null || jsonObject.getJSONObject("page").getString("page_id").length() > 0;
                    }
                })
                .within(Time.seconds(10));

        PatternStream<JSONObject> patternDS = CEP.pattern(jsonObjDS.keyBy(x -> x.getJSONObject("common").getString("mid")), targetPattern);

        OutputTag<String> timeOutTag = new OutputTag<String>("timeOutTag"){};
        SingleOutputStreamOperator<JSONObject> patternOutputDS = patternDS.flatSelect(timeOutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        collector.collect(map.getOrDefault("begin",null).iterator().next().toString());
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {
                        //不超时代码不写
                    }
                }
        );

        patternOutputDS.getSideOutput(timeOutTag).addSink(MyKafkaUtil.getKafkaSink("dwm_jump_detail"));

        env.execute();

    }
}
