package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.Label;

import java.util.EnumMap;
import java.util.Map;

public enum Labels implements Label {

    Tile,
    Territory,
    Dataset,
    Source,
    Fact,
    PopulationFact,
    UrbanPopulationFact,
    WealthFact,
    GrossTileProductionFact,
    BuiltAreaFact,
    Year,
    Computation,
    MilExFact,
    Person,
    CommonWeal,
    DiscretePolityFact,
    MissingTile

//    public final String value;
//
//    Labels(String v) {
//        value = v;
//    }
//
//    private static final EnumMap<Labels, String> _map = new EnumMap<>(Labels.class);
//
//    static {
//        for (Labels l : Labels.values()) {
//            _map.put(l, l.value);
//        }
//    }
//
//    public static String valueOf(Labels l) {
//        return _map.get(l);
//    }
//
//    public static Labels name(String v) {
//        for (Map.Entry<Labels, String> e : _map.entrySet() ) {
//            if (e.getValue() == v) {
//                return e.getKey();
//            }
//        }
//        return null;
//    }

}
