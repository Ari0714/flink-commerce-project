package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {

    private static String orderWideTopic = "dwm_order_wide";

    private static String consumerId = "comsumer041015";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/10 1.定义表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        // TODO: 2021/4/10 2.把数据源作为动态表
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                        ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                        "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                        "WATERMARK FOR rowtime AS rowtime)" +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, consumerId) + ")");

        //TODO 3.聚合计算
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code area_code," +
                "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT order_id) order_count, sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts "+
                " from ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");


        //TODO 4.转换为数据流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);


        //TODO 5.写入到 lickHouse
        provinceStatsDataStream.addSink(ClickHouseUtil.
                <ProvinceStats>getJdbcSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute();

    }


}
