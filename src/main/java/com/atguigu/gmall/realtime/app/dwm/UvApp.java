package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UvApp {

    private static String topic = "dwd_page_log";
    private static String consumerId = "consumer02";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        SingleOutputStreamOperator<JSONObject> isNewDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, consumerId))
                .map(x -> JSON.parseObject(x))
                .filter(x -> x.getJSONObject("common").getString("is_new").equals("1"));

        isNewDS.map(x -> x.toString())
                .addSink(MyKafkaUtil.getKafkaSink("dwm_uv"));


        env.execute();

    }

}
