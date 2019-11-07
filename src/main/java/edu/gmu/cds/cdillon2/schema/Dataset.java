package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

public class Dataset {
    private static final String NAME = "name";
    private final Node underlyingNode;
    private String filename;
    private double version;
    private int year;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    public Dataset(Node node) {
        this.underlyingNode = node;
    }

    public Dataset(String filename, String name, Double version, int year) {
        try (Transaction tx = db.beginTx()) {
            Node n = db.findNode(Labels.Dataset,"name", name);
            if (n==null) {
                n = db.createNode(Labels.Dataset);
                n.setProperty("name", name);
                n.setProperty("filename", filename);
                n.setProperty("version", version);
                n.setProperty("year", year);
            }
            this.underlyingNode = n;
            tx.success();
        }
    }

    public Node getUnderlyingNode() {
        return this.underlyingNode;
    }

    public String getName() {
        return (String) underlyingNode.getProperty(NAME);
    }

    public String getFilename() {
        return (String) underlyingNode.getProperty("filename");
    }

    public String getOutputFile() {
        return (String) underlyingNode.getProperty("outputFile");
    }

    public double getVersion() {
        return (Double) underlyingNode.getProperty("version");
    }

    public int getYear() {
        return (Integer) underlyingNode.getProperty("year");
    }

    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Dataset && underlyingNode.equals( ((Dataset) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Dataset[" + getName() + "]";
    }

}
