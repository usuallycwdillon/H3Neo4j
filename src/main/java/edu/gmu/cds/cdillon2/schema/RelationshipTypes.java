package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType {

    ABUTS,
    BORDERS,
    COMPUTED,
    CONTRIBUTES,
    DURING,
    INCLUDES,
    MILEX,
    OCCUPIED,
    POPULATION,
    PROVIDES,
    SIM_POPULATION,
    SIM_URBAN_POPULATION,
    SIM_WEALTH,
    URBAN_POPULATION,
    WEALTH

}
