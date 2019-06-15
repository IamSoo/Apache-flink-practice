package practice;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaConsumerTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        DataStream<String> stringDataStream =
                env.addSource(new FlinkKafkaConsumer<String>(parameterTool.getRequired("topic"), new SimpleStringSchema()
                        , parameterTool.getProperties()));

        stringDataStream.rebalance()
                .map(s -> "Soo says Hi to : " + s)
                .print();

        env.execute();
    }
}
