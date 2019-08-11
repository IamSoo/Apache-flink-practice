package practice;

import java.util.*;

import static java.util.stream.Collectors.toMap;

public class MirsaGries<T> implements iMirsaGries<T> {
    private int size;
    private Map<T, Integer> countersMap;

    public MirsaGries(int size) {
        this.size = size;
        this.countersMap = new HashMap<>();
    }

    @Override
    public void insert(T data) {
        Integer count = countersMap.get(data);
        if (count != null) {
            doAdd(data, count + 1);
        } else if (countersMap.size() < size - 1) {
            doAdd(data, 1);
        } else {
            Iterator<Map.Entry<T, Integer>> it = countersMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<T, Integer> entry = it.next();
                int c = entry.getValue();
                c -= 1;
                countersMap.put(entry.getKey(), c);
                if (c <= 0) {
                    it.remove();
                }
            }
        }
    }


    private void doAdd(T element, int count) {
        countersMap.put(element, count);
    }

    @Override
    public Map<T, Integer> frequencies() {
        return Collections.unmodifiableMap(countersMap);
    }


    @Override
    public Map<T, Integer> sortedFrequencies() {
        return Collections.unmodifiableMap(countersMap.entrySet().stream()
                .sorted(Map.Entry.<T, Integer>comparingByValue().reversed())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new)));
    }

}
