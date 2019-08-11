package practice;

import java.util.Map;

public interface IMisraGriesSummary<T> {
    public void addToSummary(T event);

    public Map<T,Integer> getEventCounterMap();
}
