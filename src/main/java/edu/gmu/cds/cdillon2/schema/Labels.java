package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.Label;

public enum Labels implements Label {

    Tile,
    Territory,
    Dataset,
    Source,
    Fact,
    PopulationFact,
    UrbanPopulationFact,
    Year,
    WealthFact,
    Computation,
    MilExFact
}
