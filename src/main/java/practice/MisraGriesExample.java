package practice;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MisraGriesExample {

    private final static int K_PARAM = 4;

    public static void main(String[] args) {
        iMirsaGries<String> frequency1 = new MirsaGries<String>(5);
        frequency1.insert("A");
        frequency1.insert("A");
        frequency1.insert("A");
        frequency1.insert("B");
        frequency1.insert("C");
        frequency1.insert("B");
        frequency1.insert("C");
        frequency1.insert("C");
        frequency1.insert("C");
        frequency1.insert("A");
        frequency1.insert("A");
        frequency1.insert("D");
        frequency1.insert("D");
        frequency1.insert("F");
        frequency1.insert("G");
        Map<String, Integer> f1 = frequency1.sortedFrequencies();
        System.out.println("freq1 is:" + f1);

        iMirsaGries<String> frequency2 = new MirsaGries<String>(5);
        frequency2.insert("D");
        frequency2.insert("D");
        frequency2.insert("D");
        frequency2.insert("B");
        frequency2.insert("C");
        frequency2.insert("B");
        frequency2.insert("F");
        frequency2.insert("C");
        frequency2.insert("C");
        frequency2.insert("F");
        frequency2.insert("A");
        frequency2.insert("D");
        frequency2.insert("E");
        frequency2.insert("F");
        frequency2.insert("G");
        Map<String, Integer> f2 = frequency2.sortedFrequencies();
        System.out.println("freq2 is:" + f2);

        Map<String, Integer> mm = mergeFrequencies(f1, f2, K_PARAM);
        System.out.println("merge summaries is: " + mm);
    }


    /**
     * merge two MG summary
     *
     * @param f1
     * @param f2
     * @param k
     * @return
     */
    public static Map<String, Integer> mergeFrequencies(Map<String, Integer> f1, Map<String, Integer> f2, int k) {
        Map<String, Integer> result = new LinkedHashMap<>(f1);
        Iterator<String> it = f2.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (result.get(key) != null) {
                result.put(key, f2.get(key) + result.get(key));
            } else {
                result.put(key, f2.get(key));
            }
        }
        int count = 0;
        count = result
                .values()
                .stream()
                .min(Integer::compare)
                .get();

        Iterator<Map.Entry<String, Integer>> iterator = result.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            int c = entry.getValue();
            c -= count;
            result.put(entry.getKey(), c);
            if (c <= 0) {
                iterator.remove();
            }
        }

        return result;
    }
}
