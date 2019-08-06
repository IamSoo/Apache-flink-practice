package MSBD5014;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class MGSummaryMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("/Users/soonamkalyan/workspace/apache-flink-practice/src/main/resources/MisraGries.input.1.txt");

        inputStream.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).process(new MisraGriesSummaryProcessFunction()).print();


        env.execute();
    }
}
