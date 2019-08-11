package practice;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.*;

public class MisraGriesSummaryCheckpointedFunction implements CheckpointedFunction {

    private static final long serialVersionId = 7035756567190539683L;
    private ValueState<Summary> summaryValueState;
    private Integer size = 3;
    private transient ListState<Tuple3<String,String, Integer>> checkpointedState;
    private List<Tuple3<String,String, Integer>> summaryElement;

   /* @Override
    public void open(Configuration parameters) {
        summaryValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("summary", Summary.class));

    }
*/
   /* @Override
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

    }*/

   /* @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Map<String, Integer>>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        Summary result = summaryValueState.value();
        out.collect(new Tuple2<String, Map<String, Integer>>(result.key, result.summaryCount));
    }*/

    public MisraGriesSummaryCheckpointedFunction() {
        this.summaryElement = new ArrayList<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Tuple3<String,String, Integer> element : summaryElement) {
            checkpointedState.add((Tuple3<String, String, Integer>) summaryElement);
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Tuple3<String,String, Integer>> descriptor =
                new ListStateDescriptor<Tuple3<String, String, Integer>>("summary-state-per-partition",
                        TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {}));

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (functionInitializationContext.isRestored()) {
            for (Tuple3<String, String,Integer> element : checkpointedState.get()) {
                summaryElement.add(element);
            }
        }
    }

    /*@Override
    public List<Tuple3<String,String, Integer>> createSummary(Tuple3<String,String, Integer> input){
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
    }*/
}
