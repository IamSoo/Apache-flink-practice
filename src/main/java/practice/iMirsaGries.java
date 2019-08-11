package practice;

import java.util.Map;

public interface iMirsaGries<T> {

    void insert(T data);

    Map<T, Integer> frequencies();

    Map<T, Integer> sortedFrequencies();
}
