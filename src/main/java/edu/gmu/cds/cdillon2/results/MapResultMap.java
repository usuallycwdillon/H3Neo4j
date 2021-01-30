package edu.gmu.cds.cdillon2.results;

import java.util.Collections;
import java.util.Map;

public class MapResultMap {
    private static final MapResultMap EMPTY = new MapResultMap(Collections.emptyMap());
    public final Map<Long, MapResult> value;

    public static MapResultMap empty() {
        return EMPTY;
    }

    public MapResultMap(Map<Long, MapResult> value) {
        this.value = value;
    }
}
