package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {

    private static String topic = "ods_base_log";
    private static String consumerId = "consumer01";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/6 1.读取数据转jsonObj
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, consumerId);
        SingleOutputStreamOperator<String> odsBaseLogDS = env.addSource(kafkaSource)
                .filter(x -> x.length() > 10);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = odsBaseLogDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        });

        // TODO: 2021/4/6 2.当日新老顾客识别
        SingleOutputStreamOperator<JSONObject> newMidDS = jsonObjDS
                .keyBy(x -> x.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    SimpleDateFormat simpleDateFormat = null;
                    ValueState<String> visitDateState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("visitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        visitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String lastVisitDate = visitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String dateStr = simpleDateFormat.format(new Date(ts));

                        if (DateTimeUtil.noNullAndEmpty(lastVisitDate)) {
                            if (!lastVisitDate.equals(dateStr)) {
                                jsonObject.getJSONObject("common").put("is_new", 1);
                                visitDateState.update(dateStr);
                            } else
                                jsonObject.getJSONObject("common").put("is_new", 0);
                        } else {
                            jsonObject.getJSONObject("common").put("is_new", 1);
                            visitDateState.update(dateStr);
                        }

                        return jsonObject;
                    }
                });

        // TODO: 2021/4/6 3.分流
        OutputTag<String> start = new OutputTag<String>("start"){};
        OutputTag<String> displays = new OutputTag<String>("displays"){};
        OutputTag<String> actions = new OutputTag<String>("actions"){};

        SingleOutputStreamOperator<String> splitDS = newMidDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                if (DateTimeUtil.noNullAndEmpty(jsonObject.getString("start"))) {
                    context.output(start, jsonObject.toString());
                } else {
                    collector.collect(jsonObject.toString());
                    if (DateTimeUtil.noNullAndEmpty(jsonObject.getString("displays")))
                        context.output(displays, jsonObject.toString());
                    else if(DateTimeUtil.noNullAndEmpty(jsonObject.getString("actions")))
                        context.output(actions, jsonObject.toString());
                }
            }
        });


        splitDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        splitDS.getSideOutput(start).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        splitDS.getSideOutput(displays).addSink(MyKafkaUtil.getKafkaSink("dwd_displays_log"));
        splitDS.getSideOutput(actions).addSink(MyKafkaUtil.getKafkaSink("dwd_actions_log"));


        env.execute();
    }

}
