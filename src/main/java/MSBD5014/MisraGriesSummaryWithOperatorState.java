package MSBD5014;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MisraGriesSummaryWithOperatorState extends RichFlatMapFunction<Long, Tuple2<Long, Integer>> implements CheckpointedFunction {

    private transient ListState<Tuple2<Long, Integer>> checkPointCountList;
    private List<Tuple2<Long, Integer>> listBufferElements;
    private final int size = 4;


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
    public void flatMap(Long inputEvent, Collector<Tuple2<Long, Integer>> collector) {
        if (listBufferElements.size() > 0) {
            boolean newlyAdded = false;
            for (int i = 0; i < listBufferElements.size(); i++) {
                if (listBufferElements.get(i).f0.equals(inputEvent)) {
                    int count;
                    count = listBufferElements.get(i).f1;
                    count++;
                    listBufferElements.set(i, Tuple2.of(listBufferElements.get(i).f0, count));
                    newlyAdded = true;
                }
            }
            if (!newlyAdded && listBufferElements.size() <= size - 1) {
                listBufferElements.add(Tuple2.of(inputEvent, 1));
            }

            if (listBufferElements.size() >= size - 1) {
                List<Tuple2<Long, Integer>> localList = new ArrayList<>();
                for (int i = 0; i < listBufferElements.size(); i++) {
                    Tuple2<Long, Integer> oldSummary = listBufferElements.get(i);
                    int innerCount = oldSummary.f1;
                    innerCount--;
                    if (innerCount != 0) {
                        localList.add(Tuple2.of(listBufferElements.get(i).f0, innerCount));
                    }
                }
                listBufferElements.clear();
                listBufferElements.addAll(localList);
            }

        } else {
            listBufferElements.add(Tuple2.of(inputEvent, 1));
        }
        Iterator<Tuple2<Long, Integer>> it = listBufferElements.iterator();
        while (it.hasNext()) {
            Tuple2<Long, Integer> oldSummary = it.next();
            collector.collect(Tuple2.of(oldSummary.f0, oldSummary.f1));
        }


    }
}
