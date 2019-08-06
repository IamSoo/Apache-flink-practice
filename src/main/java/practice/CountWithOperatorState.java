package practice;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;

public class CountWithOperatorState extends RichFlatMapFunction<Long,String> implements CheckpointedFunction {

    private transient ListState<Long> checkPointCountList;
    private List<Long> listBufferElements;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkPointCountList.clear();
        for (int i = 0 ; i < listBufferElements.size(); i ++) {
            checkPointCountList.add(listBufferElements.get(i));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<Long>(
                        "listForThree",
                        TypeInformation.of(new TypeHint<Long>() {}));
        listBufferElements = new ArrayList<>();
        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        if (functionInitializationContext.isRestored()) {
            for (Long element : checkPointCountList.get()) {
                listBufferElements.add(element);
            }
        }
    }

    @Override
    public void flatMap(Long aLong, org.apache.flink.util.Collector<String> collector) throws Exception {
        if (aLong == 1) {
            if (listBufferElements.size() > 0) {
                StringBuffer buffer = new StringBuffer();
                for(int i = 0 ; i < listBufferElements.size(); i ++) {
                    buffer.append(listBufferElements.get(i) + " ");
                }
                collector.collect(buffer.toString());
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(aLong);
        }
    }
}
