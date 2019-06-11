package practice;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AnalysisFromEnv {

    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>> wordCounts = see.fromElements(new Tuple2<String,Integer>("hello",1),
                new Tuple2<String,Integer>("hello",2));

        wordCounts.map(new MapFunction<Tuple2<String,Integer>, Integer>() {
            @Override
            public Integer map(Tuple2<String,Integer> value) throws Exception{
                return value.f1;
            }
        });

       DataStream<Tuple2<String,Integer>>  KeyedWordCounts
               =  wordCounts.keyBy(0);

        KeyedWordCounts.print();

        see.execute();

    }

}
