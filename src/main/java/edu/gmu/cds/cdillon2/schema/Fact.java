package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Fact {
    private final Node underlyingNode;
    private String name;
    private String subject;
    private String predicate;
    private String object;
    private Object value;
    private int during;

    public Fact(Node n) {
        this.underlyingNode = n;
    }

    public String getName() {
        return (String) underlyingNode.getProperty("name");
    }

    public void setName(String name) {
        this.name = name;
        underlyingNode.setProperty("name", name);
    }

    public String getSubject() {
        return (String) underlyingNode.getProperty("subject");
    }

    public void setSubject(String subject) {
        this.subject = subject;
        underlyingNode.setProperty("subject", subject);
    }

    public String getPredicate() {
        return (String) underlyingNode.getProperty("predicate");
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
        underlyingNode.setProperty("predicate", predicate);
    }

    public String getObject() {
        return (String) underlyingNode.getProperty("object");
    }

    public void setObject(String object) {
        this.object = object;
        underlyingNode.setProperty("object", object);
    }

    public Object getValue() {
        return underlyingNode.getProperty("value");
    }

    public void setValue(Object value) {
        this.value = value;
        underlyingNode.setProperty("value",value);
    }

    public int getDuring() {
        return (Integer) underlyingNode.getProperty("year");
    }

    public void setDuring(int during) {
        this.during = during;
        underlyingNode.setProperty("year", during);
    }

    public Node getUnderlyingNode() {
        return this.underlyingNode;
    }

    public Relationship getDuringRel(Node year) {
        for (Relationship rel : underlyingNode.getRelationships(RelationshipTypes.DURING)) {
            if (rel.getOtherNode(underlyingNode)==year) {
                return rel;
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
        return o instanceof Fact && underlyingNode.equals( ((Fact) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Fact[" + getSubject() + "-" + getPredicate() + "-" + getObject() + "]";
    }
}
