package practice;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CountWithOperatorExampleMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(10000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);

        SingleOutputStreamOperator<String> data =
                env.fromElements(1L, 2L, 3L, 4L, 5L, 1L, 9L, 8L, 7L, 1L)
                        .flatMap(new CountWithOperatorStateExample())
                        .timeWindowAll(Time.seconds(1))
                        .reduce(new ReduceFunction<String>() {
                            @Override
                            public String reduce(String s, String t1) throws Exception {
                                System.out.println(s + "->" + t1);
                                return s + "->" + t1;
                            }
                        });


        data.print();

        env.execute();

    }
}
