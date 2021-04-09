package com.atguigu.gmall.realtime.common;

/**
 * Author: Felix
 * Date: 2021/2/2
 * Desc: 项目配置的常量类
 */
public class GmallConfig {
    //Hbase的命名空间
    public static final String HABSE_SCHEMA = "default";
    //Phonenix连接的服务器地址
    public static final String PHOENIX_SERVER="jdbc:phoenix:hdp101,hdp102,hdp103:2181";

    //ClickHouse的URL连接地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hdp102:8123/gmall_flink";

}
