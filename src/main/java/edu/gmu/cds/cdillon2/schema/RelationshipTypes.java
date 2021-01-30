package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.RelationshipType;

import java.util.EnumMap;
import java.util.Map;

public enum RelationshipTypes implements RelationshipType {

    ABUTS,
    BORDERS,
    COMPUTED,
    CONTRIBUTES,
    CONTRIBUTES_TO,
    DURING,
    INCLUDES,
    MILEX,
    OCCUPIED,
    POPULATION,
    PROVIDES,
    SIM_POPULATION_1816,
    SIM_POPULATION_1850,
    SIM_POPULATION_1880,
    SIM_POPULATION_1914,
    SIM_POPULATION_1938,
    SIM_POPULATION_1945,
    SIM_POPULATION_1994,
    SIM_URBAN_POPULATION_1816,
    SIM_URBAN_POPULATION_1850,
    SIM_URBAN_POPULATION_1880,
    SIM_URBAN_POPULATION_1914,
    SIM_URBAN_POPULATION_1938,
    SIM_URBAN_POPULATION_1945,
    SIM_URBAN_POPULATION_1994,
    SIM_WEALTH_1816,
    SIM_WEALTH_1850,
    SIM_WEALTH_1880,
    SIM_WEALTH_1914,
    SIM_WEALTH_1938,
    SIM_WEALTH_1945,
    SIM_WEALTH_1994,
    SIM_PRODUCTION_1816,
    SIM_PRODUCTION_1850,
    SIM_PRODUCTION_1880,
    SIM_PRODUCTION_1914,
    SIM_PRODUCTION_1938,
    SIM_PRODUCTION_1945,
    SIM_PRODUCTION_1994,
    SIM_BUILT_AREA_1816,
    SIM_BUILT_AREA_1850,
    SIM_BUILT_AREA_1880,
    SIM_BUILT_AREA_1914,
    SIM_BUILT_AREA_1938,
    SIM_BUILT_AREA_1945,
    SIM_BUILT_AREA_1994,
    URBAN_POPULATION,
    WEALTH,
    PRODUCTION,
    DESCRIBES_POLITY_OF,
    KNOWS,
    REPRESENTS_POPULATION,
    RESIDES_IN

//    public final String value;
//
//    RelationshipTypes(String v) {
//        value = v;
//    }
//
//    private static final EnumMap<RelationshipTypes, String> _map = new EnumMap<>(RelationshipTypes.class);
//
//    static {
//        for (RelationshipTypes rt : RelationshipTypes.values()) {
//            _map.put(rt, rt.value);
//        }
//    }
//
//    public static String valueOf(RelationshipTypes rt) {
//        return _map.get(rt);
//    }
//
//    public static RelationshipTypes name(String v) {
//        for (Map.Entry<RelationshipTypes, String> e : _map.entrySet() ) {
//            if (e.getValue() == v) {
//                return e.getKey();
//            }
//        }
//        return null;
//    }

}
