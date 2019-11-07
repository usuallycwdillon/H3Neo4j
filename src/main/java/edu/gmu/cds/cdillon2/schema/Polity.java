package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import scala.Int;

import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;

public class Polity {
    private final Node underlyingNode;
    private String name;
    private String abb;
    private String cowcode;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

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

    public int getMilExFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(MILEX)) {
            int during = Integer.parseInt(r.getProperty("during").toString());
//            int during = (Integer) r.getProperty("during");
            if (during==year) {
                Long value = (Long) r.getOtherNode(this.underlyingNode).getProperty("value") * 1000;
                return value.intValue();
            }
        }
        return 0;
    }

    public int getPopFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(POPULATION)) {
            int during = Integer.parseInt(r.getProperty("during").toString());
//            int during = (Integer) r.getProperty("during");
            if (during==year) {
                Long value = (Long) r.getOtherNode(this.underlyingNode).getProperty("value") * 1000;
                return value.intValue();
            }
        }
        return 1;
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
