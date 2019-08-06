package practice;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CountSummaryWithOperatorState extends RichFlatMapFunction<Long, List<Tuple2<Long, Integer>>> implements CheckpointedFunction {

    private transient ListState<Tuple2<Long, Integer>> checkPointCountList;
    private List<Tuple2<Long, Integer>> listBufferElements;
    private final int size = 3;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkPointCountList.clear();
        for (int i = 0; i < listBufferElements.size(); i++) {
            checkPointCountList.add(listBufferElements.get(i));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Tuple2<Long, Integer>> listStateDescriptor =
                new ListStateDescriptor<Tuple2<Long, Integer>>(
                        "summary-of-elements",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {
                        }));

        listBufferElements = new ArrayList<>();

        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        if (functionInitializationContext.isRestored()) {
            for (Tuple2<Long, Integer> element : checkPointCountList.get()) {
                listBufferElements.add(element);
            }
        }
    }

    @Override
    public void flatMap(Long inputEvent, org.apache.flink.util.Collector<List<Tuple2<Long, Integer>>> collector) throws Exception {
        if (listBufferElements == null) {
            listBufferElements = new ArrayList<>();
        } else {
            int count = 0;
            for (Tuple2<Long, Integer> oldSummary : listBufferElements) {
                if (oldSummary.f0.equals(inputEvent)) {
                    count = oldSummary.f1;
                    count++;
                    Long key = oldSummary.f0;//same old key
                    listBufferElements.remove(oldSummary);
                    listBufferElements.add(new Tuple2<Long, Integer>(key, count));
                }
            }
            if (listBufferElements.size() < size - 1) {
                listBufferElements.add(new Tuple2<Long, Integer>(inputEvent, 1));
            } else {
                listBufferElements = new ArrayList<>();
                for (Tuple2<Long, Integer> oldSummary : listBufferElements) {
                    int innerCount = oldSummary.f1;
                    listBufferElements.add(new Tuple2<Long, Integer>(oldSummary.f0, innerCount - 1));
                }
            }
            collector.collect(listBufferElements);
        }
           /* if (listBufferElements.contains(event)) {
                        count = listBufferElements.get(event);
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
*/

    }
}
