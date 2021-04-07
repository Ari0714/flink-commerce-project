package com.atguigu.gmall.realtime.app.dws;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VisitorStatsApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");





    }

}
