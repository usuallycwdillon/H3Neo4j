package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import scala.Int;

import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;

public class Polity {
    private final Node underlyingNode;
    private String name;
    private String abb;
    private String cowcode;


    public Polity (Node node) {
        this.underlyingNode = node;
    }

    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    public String getName() {
        return (String) underlyingNode.getProperty("name");
    }

    public String getAbb() {
        return (String) underlyingNode.getProperty("abb");
    }

    public String getCowcode() {
        return (String) underlyingNode.getProperty("cowcode");
    }

    public Node getTerritoryNode(int year) {
        for (Relationship r : underlyingNode.getRelationships(OCCUPIED)) {
            int during = Integer.parseInt(r.getProperty("during").toString());
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public int getMilExFact(int y) {
        Iterable<Relationship> rels = underlyingNode.getRelationships(MILEX);
        for (Relationship r : rels) {
            int during = ((Number) r.getProperty("during")).intValue();
            if (during==y) {
                return ((Number) r.getOtherNode(this.underlyingNode).getProperty("value")).intValue();
            }
        }
        return 0;
    }

    public int getPopFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(POPULATION)) {
            int during = ((Number) r.getProperty("during")).intValue();
            if (during==year) {
                return ((Number) r.getOtherNode(this.underlyingNode).getProperty("value")).intValue();
            }
        }
        return 0;
    }

    public int getUrbanPopFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(URBAN_POPULATION)) {
            int during = ((Number) r.getOtherNode(this.underlyingNode).getProperty("during")).intValue();
            if (during==year) {
                return ((Number) r.getOtherNode(this.underlyingNode).getProperty("value")).intValue();
            }
        }
        return 0;
    }

    public Node getPolityFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(DESCRIBES_POLITY_OF)) {
//            LocalDate y = LocalDate.of(year, 01, 01);
            int from = ((LocalDate) r.getOtherNode(this.underlyingNode).getProperty("from")).getYear();
            int until = (r.getOtherNode(this.underlyingNode).hasProperty("until")) ?  (
                    (LocalDate) r.getOtherNode(this.underlyingNode).getProperty("until")).getYear() : 2050;
            if (from < year && until >= year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }


    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Polity && underlyingNode.equals( ((Polity) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Polity[" + getName() + "]";
    }



}
