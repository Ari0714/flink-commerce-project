package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.phoenix.util.DateUtil;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

public class ProductStatsApp {

    // TODO: 2021/4/9 1.消费主题：点击 曝光 收藏 加入购物车  下单 支付 退款 评价
    private static String clickTopic = "dwd_";
    private static String pageTopic = "dwd_page_log";
    private static String favorTopic = "dwd_favor_info";
    private static String cartTopic = "dwd_cart_info";

    private static String orderTopic = "dwm_order_wide";
    private static String paymentTopic = "dwm_payment_wide";
    private static String orderRefundTopic = "dwd_order_refund_info";
    private static String commentTopic = "dwd_comment_info";

    private static String consumerId = "consumer041010";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行要慎重 造成窗口提前关闭
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hdp101:9000/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO: 2021/4/9 2.获取流
        DataStreamSource<String> kafkaClick = env.addSource(MyKafkaUtil.getKafkaSource(clickTopic, consumerId));
        DataStreamSource<String> kafkaPage = env.addSource(MyKafkaUtil.getKafkaSource(pageTopic, consumerId));
        DataStreamSource<String> kafkaFavor = env.addSource(MyKafkaUtil.getKafkaSource(favorTopic, consumerId));
        DataStreamSource<String> kafkaCart = env.addSource(MyKafkaUtil.getKafkaSource(cartTopic, consumerId));

        DataStreamSource<String> kafkaOrder = env.addSource(MyKafkaUtil.getKafkaSource(orderTopic, consumerId));
        DataStreamSource<String> kafkaPayment = env.addSource(MyKafkaUtil.getKafkaSource(paymentTopic, consumerId));
        DataStreamSource<String> kafkaOrderRefund = env.addSource(MyKafkaUtil.getKafkaSource(orderRefundTopic, consumerId));
        DataStreamSource<String> kafkaComment = env.addSource(MyKafkaUtil.getKafkaSource(commentTopic, consumerId));


//        kafkaPage.print("page");
//        kafkaFavor.print("favor");
//        kafkaCart.print("cart");
//
//        kafkaOrder.print("order");
//        kafkaPayment.print("pay");
//        kafkaOrderRefund.print("refund");
//        kafkaComment.print("comment");

        // TODO: 2021/4/10 2.结构转换
        //    String stt;//窗口起始时间
        //    String edt;  //窗口结束时间
        //    Long sku_id; //sku编号
        //    String sku_name;//sku名称
        //    BigDecimal sku_price; //sku单价
        //    Long spu_id; //spu编号
        //    String spu_name;//spu名称
        //    Long tm_id; //品牌编号
        //    String tm_name;//品牌名称
        //    Long category3_id;//品类编号
        //    String category3_name;//品类名称
        //
        //    @Builder.Default
        //    Long display_ct = 0L; //曝光数
        //
        //    @Builder.Default
        //    Long click_ct = 0L;  //点击数
        //
        //    @Builder.Default
        //    Long favor_ct = 0L; //收藏数
        //
        //    @Builder.Default
        //    Long cart_ct = 0L;  //添加购物车数
        //
        //    @Builder.Default
        //    Long order_sku_num = 0L; //下单商品个数
        //
        //    @Builder.Default   //下单商品金额
        //    BigDecimal order_amount = BigDecimal.ZERO;
        //
        //    @Builder.Default
        //    Long order_ct = 0L; //订单数
        //
        //    @Builder.Default   //支付金额
        //    BigDecimal payment_amount = BigDecimal.ZERO;
        //
        //    @Builder.Default
        //    Long paid_order_ct = 0L;  //支付订单数
        //
        //    @Builder.Default
        //    Long refund_order_ct = 0L; //退款订单数
        //
        //    @Builder.Default
        //    BigDecimal refund_amount = BigDecimal.ZERO;
        //
        //    @Builder.Default
        //    Long comment_ct = 0L;//评论数
        //
        //    @Builder.Default
        //    Long good_comment_ct = 0L; //好评数
        //
        //    @Builder.Default
        //    @TransientSink
        //    Set orderIdSet = new HashSet();  //用于统计订单数
        //
        //    @Builder.Default
        //    @TransientSink
        //    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数
        //
        //    @Builder.Default
        //    @TransientSink
        //    Set refundOrderIdSet = new HashSet();//用于退款支付订单数
        //
        //    Long ts; //统计时间戳
        SingleOutputStreamOperator<ProductStats> pageProductStats = kafkaPage.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String s, Context context, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                Long Ts = jsonObject.getLong("ts");
                JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                String pageId = pageJsonObj.getString("page_id");

                if ("good_detail".equals(pageId)) {
                    Long skuId = pageJsonObj.getLong("item");
                    ProductStats productStats = ProductStats.builder().sku_id(skuId).ts(Ts).click_ct(1L).build();
                    collector.collect(productStats);
                }

                JSONArray displays = jsonObject.getJSONArray("displays");
                if (DateTimeUtil.noNullAndEmpty(displays.toJSONString())) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            Long skuId = display.getLong("item");
                            ProductStats productStats = ProductStats.builder().ts(Ts).sku_id(skuId).click_ct(1L).build();
                            collector.collect(productStats);
                        }
                    }
                }
            }
        });


        SingleOutputStreamOperator<ProductStats> orderProductStats = kafkaOrder.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                OrderWide orderWide = JSON.parseObject(s, OrderWide.class);
                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(orderWide.getCreate_ts()).build();
            }
        });

        SingleOutputStreamOperator<ProductStats> favorProductStats = kafkaFavor.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s).getJSONObject("data");
                Long Ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                return ProductStats.builder()
                        .ts(Ts)
                        .favor_ct(1L)
                        .sku_id(jsonObject.getLong("sku_id"))
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> cartProductStats = kafkaCart.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s).getJSONObject("data");
                Long Ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                return ProductStats.builder()
                        .ts(Ts)
                        .cart_ct(1L)
                        .sku_id(jsonObject.getLong("sku_id"))
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> payProductStats = kafkaPayment.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(s, PaymentWide.class);
                Long Ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getTotal_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(Ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> orderRefundProductStats = kafkaOrderRefund.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s).getJSONObject("data");
                Long Ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(new HashSet(Collections.singleton(jsonObject.getLong("order_id"))))
                        .ts(Ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> commentProductStats = kafkaComment.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s).getJSONObject("data");
                Long Ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                Long goodCt = GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise")) ? 1L : 0L;
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(Ts)
                        .build();
            }
        });


        //page:7> ProductStats(stt=null, edt=null, sku_id=2, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=1, favor_ct=0, cart_ct=0, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=0, good_comment_ct=0, orderIdSet=[], paidOrderIdSet=[], refundOrderIdSet=[], ts=1608278310000)
//        pageProductStats.print("page");

        //order:8> ProductStats(stt=null, edt=null, sku_id=23, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=0, favor_ct=0, cart_ct=0, order_sku_num=2, order_amount=80.00, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=0, good_comment_ct=0, orderIdSet=[38219], paidOrderIdSet=[], refundOrderIdSet=[], ts=1589731200000)
//        orderProductStats.print("order");

        //pay:8> ProductStats(stt=null, edt=null, sku_id=14, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=0, favor_ct=0, cart_ct=0, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=48432.00, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=0, good_comment_ct=0, orderIdSet=[], paidOrderIdSet=[38231], refundOrderIdSet=[], ts=1589738919000)
//        payProductStats.print("pay");

        //favor:4> ProductStats(stt=null, edt=null, sku_id=25, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=0, favor_ct=1, cart_ct=0, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=0, good_comment_ct=0, orderIdSet=[], paidOrderIdSet=[], refundOrderIdSet=[], ts=1589738763000)

//        favorProductStats.print("favor");

        //cart:2> ProductStats(stt=null, edt=null, sku_id=17, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=0, favor_ct=0, cart_ct=1, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=0, good_comment_ct=0, orderIdSet=[], paidOrderIdSet=[], refundOrderIdSet=[], ts=1589736057000)
//        cartProductStats.print("cart");

        //comment:5> ProductStats(stt=null, edt=null, sku_id=8, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=0, favor_ct=0, cart_ct=0, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=0, comment_ct=1, good_comment_ct=0, orderIdSet=[], paidOrderIdSet=[], refundOrderIdSet=[], ts=1589736323000)
//        commentProductStats.print("comment");

        //refund:3> ProductStats(stt=null, edt=null, sku_id=19, sku_name=null, sku_price=null, spu_id=null, spu_name=null, tm_id=null, tm_name=null, category3_id=null, category3_name=null, display_ct=0, click_ct=0, favor_ct=0, cart_ct=0, order_sku_num=0, order_amount=0, order_ct=0, payment_amount=0, paid_order_ct=0, refund_order_ct=0, refund_amount=11999.00, comment_ct=0, good_comment_ct=0, orderIdSet=[], paidOrderIdSet=[], refundOrderIdSet=[42279], ts=1589738994000)
//        orderRefundProductStats.print("refund");


        // TODO: 2021/4/10 3.union mark 分组 开窗 reduce
        DataStream<ProductStats> unionDS = pageProductStats.union(orderProductStats, favorProductStats, cartProductStats, commentProductStats, orderRefundProductStats, payProductStats);


        SingleOutputStreamOperator<ProductStats> markDS = unionDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ProductStats>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(ProductStats productStats) {
                return productStats.getTs();
            }
        });

//        unionDS.print("union");

        //商品id分组  开窗 聚合    //reduceProductStats
        KeyedStream<ProductStats, Long> keyedDS = markDS.keyBy(new KeySelector<ProductStats, Long>() {
            @Override
            public Long getKey(ProductStats productStats) throws Exception {
                return productStats.getSku_id();
            }
        });

//        keyedDS.print("key");

        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(new ReduceFunction<ProductStats>() {
                                                                                @Override
                                                                                public ProductStats reduce(ProductStats pro1, ProductStats pro2) throws Exception {
                                                                                    pro1.setDisplay_ct(pro1.getDisplay_ct() + pro2.getDisplay_ct());
                                                                                    pro1.setClick_ct(pro1.getClick_ct() + pro2.getClick_ct());
                                                                                    pro1.setCart_ct(pro1.getCart_ct() + pro2.getCart_ct());
                                                                                    pro1.setFavor_ct(pro1.getFavor_ct() + pro2.getFavor_ct());
                                                                                    pro1.setOrder_amount(pro1.getOrder_amount().add(pro2.getOrder_amount()));
                                                                                    pro1.getOrderIdSet().addAll(pro2.getOrderIdSet());
                                                                                    pro1.setOrder_ct(pro1.getOrderIdSet().size() + 0L);
                                                                                    pro1.setOrder_sku_num(pro1.getOrder_sku_num() + pro2.getOrder_sku_num());
                                                                                    pro1.setPayment_amount(pro1.getPayment_amount().add(pro2.getPayment_amount()));
                                                                                    pro1.getRefundOrderIdSet().addAll(pro2.getRefundOrderIdSet());
                                                                                    pro1.setRefund_order_ct(pro1.getRefundOrderIdSet().size() + 0L);
                                                                                    pro1.setRefund_amount(pro1.getRefund_amount().add(pro2.getRefund_amount()));
                                                                                    pro1.getPaidOrderIdSet().addAll(pro2.getPaidOrderIdSet());
                                                                                    pro1.setPaid_order_ct(pro1.getPaidOrderIdSet().size() + 0L);

                                                                                    pro1.setComment_ct(pro1.getComment_ct() + pro2.getComment_ct());
                                                                                    pro1.setGood_comment_ct(pro1.getGood_comment_ct() + pro2.getGood_comment_ct());
//                                                                                    System.out.println("pro: " + pro1);
                                                                                    return pro1;
                                                                                }
                                                                            },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats productStats : iterable) {
                            productStats.setStt(simpleDateFormat.format(context.window().getStart()));
                            productStats.setEdt(simpleDateFormat.format(context.window().getEnd()));
                            productStats.setTs(new Date().getTime());
                            collector.collect(productStats);
                        }
                    }
                }
        );

        // TODO: 2021/4/10 4.关联维度信息
        SingleOutputStreamOperator<ProductStats> withDS = reduceDS.map(new RichMapFunction<ProductStats, ProductStats>() {

            Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = PhoenixUtil.getConn();
            }

            @Override
            public ProductStats map(ProductStats productStats) throws Exception {

                //sku表    Long spu_id;   Long tm_id;  Long category3_id;  String spu_name;  String tm_name;  String category3_name;
                Long skuId = productStats.getSku_id();
                JSONObject skuJsonObj = PhoenixUtil.queryOne("select * from GMALL2020_SKU_INFO where ID = '" + skuId + "'", connection);
                productStats.setSku_name(skuJsonObj.getString("SKU_NAME"));
                productStats.setSku_price(skuJsonObj.getBigDecimal("PRICE"));
                productStats.setSpu_id(skuJsonObj.getLong("SPU_ID"));
                productStats.setSpu_name(skuJsonObj.getString("SPU_NAME"));
                productStats.setTm_id(skuJsonObj.getLong("TM_ID"));
                productStats.setTm_name(skuJsonObj.getString("TM_NAME"));
                productStats.setCategory3_id(skuJsonObj.getLong("CATEGORY3_ID"));
                productStats.setCategory3_name(skuJsonObj.getString("CATEGORY3_NAME"));
                return productStats;
            }
        });

        withDS.print("reduce");

        withDS.addSink(ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        env.execute();


    }

}
