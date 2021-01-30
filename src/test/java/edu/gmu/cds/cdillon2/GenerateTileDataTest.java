package edu.gmu.cds.cdillon2;

import edu.gmu.cds.cdillon2.results.StringResult;
import edu.gmu.cds.cdillon2.wog.Wog;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;

/**
 * Unit test for generateTileData using Territory 'France of 1816'.
 */
public class GenerateTileDataTest {
//    // the rule starts the neo4j instance
//    @Rule
//    public Neo4jRule rule = new Neo4jRule().withProcedure(Wog.class);

//    @Test
//    public void generateTileDataTest() {
//        try(Driver driver = GraphDatabase.driver(rule.boltURI(), Config.build().withoutEncryption().toConfig() )) {
//            Session session = driver.session();
//            String statement = "MATCH (t:Territory{mapKey:\"France of 1816\"}) " +
//                    "MERGE (c:Computation{filename:\"Wog.java\", " +
//                    "time:datetime({ timezone: 'UTC' }), name:\"WOG\", methodname:\"wog.test.simulateTileFacts\" }) " +
//                    "WITH t, c CALL wog.simulateTileFacts(t, c) YIELD value RETURN value";
//            StatementResult result = session.run(statement);
//            System.out.println(result.single().toString());
//        }
//    }

//    @Test
//    public void findTestTerritory() {
//        try (Driver driver = GraphDatabase.driver(rule.boltURI(), Config.build().withoutEncryption().toConfig())) {
//            Session session = driver.session();
//            StatementResult result = session.run("MATCH (t:Territory{mapKey:\"France of 1816\"}) RETURN t");
//            System.out.println(result.single().toString());
//        }
//    }

}
