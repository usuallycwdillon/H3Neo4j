package edu.gmu.cds.cdillon2.schema;

import org.neo4j.graphdb.Node;

import java.time.LocalDate;

public class Year {
    private final Node underlyingNode;
    private static final String NAME = "name";
    private LocalDate lastWeekBegins;

    public Year(Node n) {
        this.underlyingNode = n;
    }

    public String getName() {
        return (String) underlyingNode.getProperty(NAME);
    }

    public Node getUnderlyingNode() {
        return this.underlyingNode;
    }

    public LocalDate getLastWeekBegins() {
        return (LocalDate) underlyingNode.getProperty("lastWeekBegins");
    }

    public Integer getYearAsInt() {
        LocalDate ld = this.getLastWeekBegins();
        return ld.getYear();
    }


    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Year && underlyingNode.equals( ((Year) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Year[" + getName() + "]";
    }
}
