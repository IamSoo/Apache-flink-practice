package MSBD5014;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class MisraGriesMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment
                env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<String> dataStream = loadDataFromFile(env);
        env.setParallelism(3);
        DataStream<Tuple2<Long, Integer>> flatMapSummary =
                dataStream.map(new MapFunction<String, Long>() {
                    @Override
                    public Long map(String s) throws Exception {
                        return Long.valueOf(s);
                    }
                })
         .flatMap(new MisraGriesSummaryWithOperatorState()).setParallelism(1);

        DataStream<Map<Long, Integer>> mapDataStream =
                flatMapSummary.timeWindowAll(Time.seconds(3))
                        .apply(new AllWindowFunction<Tuple2<Long, Integer>, Map<Long, Integer>, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, Integer>> iterable, Collector<Map<Long, Integer>> collector) throws Exception {
                                int k = 4;
                                Map<Long, Integer> resultMap = new HashMap<>();
                                for (Tuple2<Long, Integer> eachTuple : iterable) {
                                    resultMap.put(eachTuple.f0, eachTuple.f1);
                                }
                                int count = 0;
                                //count the least value
                                count = resultMap
                                        .values()
                                        .stream()
                                        .min(Integer::compare)
                                        .get();

                                Iterator<Map.Entry<Long, Integer>> iterator = resultMap.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<Long, Integer> entry = iterator.next();
                                    int c = entry.getValue();
                                    c -= count;
                                    resultMap.put(entry.getKey(), c);
                                    if (c <= 0) {
                                        iterator.remove();
                                    }

                                }
                                collector.collect(resultMap);
                            }
                        });
        mapDataStream.print();
        env.execute();
    }

    private static DataStream<String> loadDataFromFile(final StreamExecutionEnvironment env) {
        DataStream<String> data = env.readTextFile("/Users/soonamkalyan/workspace/apache-flink-practice/src/main/resources/MisraGries.input.1.txt");
        return data;
    }

}
