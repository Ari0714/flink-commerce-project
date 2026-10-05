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
        env.setParallelism(1);
        DataStreamSource<String> inputDS = env.readTextFile("input/article.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wcDS = inputDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] strings = s.split(" ");
                        for (String string : strings) {
                            collector.collect(new Tuple2<>(string, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);
        wcDS.print();

        env.execute();

    }
}

