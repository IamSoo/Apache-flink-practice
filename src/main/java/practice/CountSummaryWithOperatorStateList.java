package practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class CountSummaryWithOperatorStateList extends RichFlatMapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>  {
    private static final long serialVersionUID = -4780146677198295204L;
    private MapState<Long, Integer> modelState;
    private final int size = 3;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Integer> typeInformation = TypeInformation
                .of(new TypeHint<Integer>() {
                });
        MapStateDescriptor<Long, Integer> descriptor;
        descriptor = new MapStateDescriptor<Long, Integer>("modelState", TypeInformation.of(Long.class), typeInformation);
        modelState = getRuntimeContext().getMapState(descriptor);
        //this.modelState = getRuntimeContext().getMapState(new MapStateDescriptor("modelState", Long.class, Integer.class));

    }

   @Override
    public void flatMap(Tuple2<Long, Integer> longIntegerTuple2, Collector<Tuple2<Long, Integer>> collector) throws Exception {
            int count = 0;
            int totalCounts = 0;
              Iterator<Map.Entry<Long, Integer>> counterIt =  modelState.iterator();
              if(counterIt.hasNext()){
                  totalCounts++;
              }
            if(modelState.contains(longIntegerTuple2.f0) ){
                    count = modelState.get(longIntegerTuple2.f0);
                    count++;
                    modelState.put(longIntegerTuple2.f0, count);
             } else if(totalCounts < size -1 ){
                modelState.put(longIntegerTuple2.f0, 1);
            }else {
                    Iterator<Map.Entry<Long, Integer>> iterator = modelState.entries().iterator();
                    while (iterator.hasNext()) {
                        int localCounter;
                        Map.Entry<Long, Integer> eventCounterEntry = iterator.next();
                        localCounter = eventCounterEntry.getValue();
                        localCounter -= 1;
                        modelState.put(eventCounterEntry.getKey(), localCounter);
                        if (localCounter == 0) {
                            iterator.remove();
                        }
            }
            }
       collector.collect(Tuple2.of(longIntegerTuple2.f0,modelState.get(longIntegerTuple2.f0)));
        }


    /*@Override
    public Map<Long, Integer> map(Long inputEvent) throws Exception {
        int count = 0;
        if (modelState.contains(inputEvent)) {
            count = modelState.get(inputEvent);
            count++;
            modelState.put(inputEvent, count);
        } else {
            Iterator<Map.Entry<Long, Integer>> iterator = modelState.entries().iterator();
            while (iterator.hasNext()) {
                int localCounter;
                Map.Entry<Long, Integer> eventCounterEntry = iterator.next();
                localCounter = eventCounterEntry.getValue();
                localCounter -= 1;
                modelState.put(eventCounterEntry.getKey(), localCounter);
                if (localCounter == 0) {
                    iterator.remove();
                }
            }

        }
        return new HashMap((Map) modelState.entries());
    }*/



    }
