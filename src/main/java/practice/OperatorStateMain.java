package practice;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class OperatorStateMain {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment
                env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置checkpoint
        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(10000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(3)
         .fromElements(1L,2L,3L,4L,5L,1L,3L,4L,5L,6L,7L,1L,4L,5L,3L,9L,9L,2L,1L)
               // .print();
               .flatMap(new CountSummaryWithOperatorState())
                .print();



        env.execute();
    }
}
