package MSBD5014;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MisraGries {


    private int maxSize = 1;

    public void MisraGries(int maxSize) {
        this.maxSize = maxSize;
    }

    Map<Object, Integer> data = new HashMap<>();


    private static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputData = loadDataFromFile(args, env);
        }

    public  Map<Object, Integer>  updateSummary(Object weight) {
        int counter = 0;
        if (data.containsKey(weight)) {
            counter = data.get(weight) + 1;
            data.put(weight, counter);
        } else {
            if (data.size() < maxSize - 1) {
                data.put(weight, 1);
            } else {
                Iterator<Map.Entry<Object, Integer>> iterator = data.entrySet().iterator();
                counter = data.get(weight) - 1;
                if (counter <= 0) {
                    iterator.remove();
                } else {
                    data.put(weight, counter);
                }

            }
        }
        return data;
    }

    public void mergeSummary(Map<Object, Integer> summary1,Map<Object, Integer> summary2) {
        Map<Object, Integer> finalSummary = new HashMap<>(summary1);
        summary2.forEach((key,value) -> {
            finalSummary.merge(key,value,(v1, v2) -> v1+v2);
        });

    }

    public Object getFrequentItem(Map<Object, Integer> summary,Object key){
        if(summary.containsKey(key)){
            return summary.get(key);
        }else{
            return 0;
        }
    }


    private static DataStream<String> loadDataFromFile(String[] args, final StreamExecutionEnvironment env) {
        DataStream<String> data = env.readTextFile("/Users/soonam/workspace/flinkpractice/src/main/resources/MisraGries.input.1.txt");
        return data;
    }

}
