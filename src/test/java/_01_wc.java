import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.util.Collector;

public class _01_wc {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("D:\\study\\IDEA\\commerce_flink_java\\src\\main\\resources\\article.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = inputDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] strings = s.split(" ");
                        for (String string : strings) {
                            collector.collect(new Tuple2<>(string, 1));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                    }
                });


        env.execute();

    }
}

