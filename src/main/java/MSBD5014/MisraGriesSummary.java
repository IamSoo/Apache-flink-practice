package MSBD5014;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MisraGriesSummary<T> implements IMisraGriesSummary<T> {

    private Map<T, Integer> eventCounterMap;
    private int size;

    public MisraGriesSummary(int size){
        this.size = size;
        this.eventCounterMap = new HashMap<>();
    }

    public Map<T, Integer> getEventCounterMap(){
        return this.eventCounterMap;
    }

    @Override
    public void addToSummary(T event) {
        //if event is present in the eventCounterMap then increment the counter
        int count = 0;
        if (eventCounterMap.containsKey(event)) {
            count = eventCounterMap.get(event);
            count++;
            updateEventCountMap(event, count);
        } else if (eventCounterMap.size() < size - 1) {
            updateEventCountMap(event, 1);
        } else {
            Iterator<Map.Entry<T, Integer>> iterator = eventCounterMap.entrySet().iterator();
            while (iterator.hasNext()) {
                int localCounter;
                Map.Entry<T, Integer> eventCounterEntry = iterator.next();
                localCounter = eventCounterEntry.getValue();
                localCounter -= 1;
                updateEventCountMap(eventCounterEntry.getKey(), localCounter);
                if (localCounter == 0) {
                    iterator.remove();
                }
            }
        }
    }

    private void updateEventCountMap(T event, Integer counter) {
        eventCounterMap.put(event, counter);
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputData = loadDataFromFile(args, env);

        IMisraGriesSummary iMisraGriesSummary = new MisraGriesSummary(3);
        iMisraGriesSummary.addToSummary("A");
        iMisraGriesSummary.addToSummary("A");

        iMisraGriesSummary.addToSummary("B");
        iMisraGriesSummary.addToSummary("B");
        iMisraGriesSummary.addToSummary("A");
        iMisraGriesSummary.addToSummary("C");
        iMisraGriesSummary.addToSummary("C");
        iMisraGriesSummary.addToSummary("D");

        iMisraGriesSummary.getEventCounterMap().forEach((K,V) -> System.out.println("Key is : " + K + " Value is : " + V));

       /* DataStream<String> convertedData = inputData.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) {
                System.out.printf("Input is : " + input);
                return input;
            }
        });*//*
        env.execute();*/

    }

    private static DataStream<String> loadDataFromFile(String[] args, final StreamExecutionEnvironment env) {
        DataStream<String> data = env.readTextFile("/Users/soonamkalyan/workspace/apache-flink-practice/src/main/resources/MisraGries.input.1.txt");
        return data;
    }


}
