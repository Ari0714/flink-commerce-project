package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class VisitorStatsApp {

    private static String pageLogTopic = "dwd_page_log";
    private static String uvTopic = "dwm_uv";
    private static String jumpTopic = "dwm_jump_detail";

    private static String comsumerId = "consumer0409-02";

    private static String sinkTopic = "dwd_page_log";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/9 1.获取流
        DataStreamSource<String> kafkaPageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(pageLogTopic, comsumerId));
        DataStreamSource<String> kafkaUvDS = env.addSource(MyKafkaUtil.getKafkaSource(uvTopic, comsumerId));
        DataStreamSource<String> kafkaJumpDS = env.addSource(MyKafkaUtil.getKafkaSource(jumpTopic, comsumerId));


//        kafkaPageLogDS.print("page");
//        kafkaUvDS.print("uv");
//        kafkaJumpDS.print("jump");


        // TODO: 2021/4/9 2.为union先转换格式为visitorstats
        //    private String stt;
        //    //统计结束时间
        //    private String edt;
        //    //维度：版本
        //    private String vc;
        //    //维度：渠道
        //    private String ch;
        //    //维度：地区
        //    private String ar;
        //    //维度：新老用户标识
        //    private String is_new;
        //    //度量：独立访客数
        //    private Long uv_ct=0L;
        //    //度量：页面访问数
        //    private Long pv_ct=0L;
        //    //度量： 进入次数 (session_count)
        //    private Long sv_ct=0L;
        //    //度量： 跳出次数
        //    private Long uj_ct=0L;
        //    //度量： 持续访问时间
        //    private Long dur_sum=0L;
        //    //统计时间
        //    private Long ts;
        SingleOutputStreamOperator<VisitorStats> pvVisitorStats = kafkaPageLogDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 1L, 0L, 0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });

        SingleOutputStreamOperator<VisitorStats> uvVisitorStats = kafkaUvDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        1L, 0L, 0L, 0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });

        //sv session_count  进入页面数：last_page_id为null
        SingleOutputStreamOperator<VisitorStats> svVisitorstats = kafkaPageLogDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (DateTimeUtil.nullOrEmpty(lastPageId)) {
                    return new VisitorStats(
                            "", "",
                            jsonObject.getJSONObject("common").getString("vc"),
                            jsonObject.getJSONObject("common").getString("ch"),
                            jsonObject.getJSONObject("common").getString("ar"),
                            jsonObject.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L,
                            jsonObject.getJSONObject("page").getLong("during_time"),
                            jsonObject.getLong("ts")
                    );
                }
                return null;
            }
        });

        SingleOutputStreamOperator<VisitorStats> jumpVisitorstats = kafkaJumpDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 0L, 0L, 1L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
            }
        });


        // TODO: 2021/4/9 3. union
        DataStream<VisitorStats> unionVisitorstats = pvVisitorStats.union(uvVisitorStats, svVisitorstats, jumpVisitorstats);

//        unionVisitorstats.print("union");


        // TODO: 2021/4/9 4.watermark
        SingleOutputStreamOperator<VisitorStats> markDS = unionVisitorstats.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<VisitorStats>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(VisitorStats visitorStats) {
                return visitorStats.getTs();
            }
        });

        // TODO: 2021/4/9 5.分组取4个维度
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyDS = markDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<>(
                        visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new()
                );
            }
        });

        // TODO: 2021/4/9 6.开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> winDS = keyDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // TODO: 2021/4/9 7.窗口聚合 补充时间字段
        SingleOutputStreamOperator<VisitorStats> reduceAndDateDS = winDS.reduce(new ReduceFunction<VisitorStats>() {
                                                                           @Override
                                                                           public VisitorStats reduce(VisitorStats st1, VisitorStats st2) throws Exception {
                                                                               st1.setPv_ct(st1.getPv_ct() + st2.getPv_ct());
                                                                               st1.setUv_ct(st1.getUv_ct() + st2.getUv_ct());
                                                                               st1.setSv_ct(st1.getSv_ct() + st2.getSv_ct());
                                                                               st1.setUj_ct(st1.getUj_ct() + st2.getUj_ct());
                                                                               return st1;
                                                                           }
                                                                       },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : iterable) {
                            String startDate = simpleDateFormat.format(new Date(context.window().getStart()));
                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            collector.collect(visitorStats);
                        }
                    }
                }
        );

//        reduceAndDateDS.print("reduce");

        // TODO: 2021/4/9 8.写入数据库
        reduceAndDateDS.addSink(ClickHouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        env.execute();

    }

}
