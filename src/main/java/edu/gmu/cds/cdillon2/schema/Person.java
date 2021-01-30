package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

public class Person {
    private static final String NAME = "name";
    private final Node underlyingNode;
    private String address;
    private String birthplace;
    private double bcScore;
    private boolean leaderRole;

    public Log log;


    public Person(Node node) {
        this.underlyingNode = node;
    }


    public String getName() {
        return (String) underlyingNode.getProperty(NAME);
    }

    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    public String getAddress() {
        return (String) underlyingNode.getProperty("address");
    }

    public String getObjectAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getBirthplace() {
        return (String) underlyingNode.getProperty("birthplace");
    }

    public void setBirthplace(String birthplace) {
        this.birthplace = birthplace;
        this.underlyingNode.setProperty("birthplace", birthplace);
    }

    public double getBcScore() {
        return (Double) underlyingNode.getProperty("bcScore");
    }

    public void setBcScore(double bcScore) {
        this.bcScore = bcScore;
    }

    public boolean isLeaderRole() {
        return (Boolean) underlyingNode.getProperty("leadershipRole");
    }

    public void setLeaderRole(boolean leaderRole) {
        this.leaderRole = leaderRole;
    }

    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Person && underlyingNode.equals( ((Person) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Person[" + getName() + " with betweenness " + getBcScore() + " from " + getAddress() + " in " + getBirthplace() + "]";
    }

}
