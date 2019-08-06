package MSBD5014;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MisraGriesSummaryProcessFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Map<String, Integer>>> {

    private static final long serialVersionId = 7035756567190539683L;
    private ValueState<Summary> summaryValueState;
    private Integer size = 3;

    @Override
    public void open(Configuration parameters) {
        summaryValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("summary", Summary.class));

    }

    @Override
    public void processElement(Tuple2<String, Integer> input, Context context, Collector<Tuple2<String, Map<String, Integer>>> collector) throws Exception {
        Summary summaryOnEachNode = summaryValueState.value();
        //Think first time we are running something
        if (summaryOnEachNode == null) {
            summaryOnEachNode = new Summary();
            summaryOnEachNode.summaryCount = new HashMap<>();
        } else {
            Map<String, Integer> eventCounterMap = summaryOnEachNode.summaryCount;
            String event = input.f0;
            int count = 0;
            if (eventCounterMap.containsKey(event)) {
                count = eventCounterMap.get(event);
                count++;
                eventCounterMap.put(event, count);
                summaryValueState.update(summaryOnEachNode);
            } else if (eventCounterMap.size() < size - 1) {
                eventCounterMap.put(event, 1);
                summaryValueState.update(summaryOnEachNode);
            } else {
                Iterator<Map.Entry<String, Integer>> iterator = eventCounterMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    int localCounter;
                    Map.Entry<String, Integer> eventCounterEntry = iterator.next();
                    localCounter = eventCounterEntry.getValue();
                    localCounter -= 1;
                    eventCounterMap.put(eventCounterEntry.getKey(), localCounter);
                    if (localCounter == 0) {
                        iterator.remove();
                    }
                }
            }
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Map<String, Integer>>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        Summary result = summaryValueState.value();
        out.collect(new Tuple2<String, Map<String, Integer>>(result.key, result.summaryCount));
    }

}
