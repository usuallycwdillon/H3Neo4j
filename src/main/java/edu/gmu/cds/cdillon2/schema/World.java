package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.List;
import java.util.Map;

public class World {
    private static final String NAME = "name";
    private final Node underlyingNode;
    private Map<String, Node> tiles;
    private Map<String, Node> territories;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    public World(Node node) {
        this.underlyingNode = node;
    }

    public Node getUnderlyingNode() {
        return this.underlyingNode;
    }

    public String getName() {
        return (String) underlyingNode.getProperty(NAME);
    }

    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof World && underlyingNode.equals( ((World) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "World[" + getName() + "]";
    }


}
