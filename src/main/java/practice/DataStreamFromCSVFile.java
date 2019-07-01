package practice;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

import java.util.stream.Collector;

public class DataStreamFromCSVFile {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> output =
                 env.readTextFile("/Users/soonam/workspace/flinkpractice/src/main/resources/people.csv")
                .map(new SexMapper())
                .keyBy(0)
                .sum(1);

        output.print();

        env.execute();


    }

    public static class SexMapper implements MapFunction<String, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            String [] splitedLine = s.split(",");
            return new Tuple2<String,Integer>(splitedLine[3],1);
        }
    }
}
