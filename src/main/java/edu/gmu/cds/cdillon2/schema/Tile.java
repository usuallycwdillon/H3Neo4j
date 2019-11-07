package edu.gmu.cds.cdillon2.schema;

import com.uber.h3core.H3Core;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;


public class Tile {
    private static final String ADDRESS = "address";
    private final Node underlyingNode;
    private Long h3Id;
    private int face;
    private int resolution;
    private List<String> neighbors = new ArrayList<>();
    private int population;
    private int urbanPopulation;
    private int year;
    private double wealth;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    public Tile(Node node) {
        this.underlyingNode = node;
    }

    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    public void configureTile() {
        try {
            H3Core h3 = H3Core.newInstance();
            String h3address = (String) underlyingNode.getProperty(ADDRESS);
            this.h3Id = h3.stringToH3(h3address);
            this.face = h3.h3GetBaseCell(h3address);
            this.resolution = h3.h3GetResolution(h3address);
            this.neighbors = h3.hexRing(h3address, 1);
            this.neighbors.remove(h3address);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getAddress() {
        return (String) underlyingNode.getProperty("address");
    }

    public Long getH3Id() {
        return h3Id;
    }

    public int getFace() {
        return face;
    }

    public int getResolution() {
        return resolution;
    }

    public List<String> getNeighbors() {
        return neighbors;
    }

    public int getPopulation() {
        return population;
    }

    public void setPopulation(int pop) {
        this.population = pop;
    }

    public int getUrbanPopulation() {
        return urbanPopulation;
    }

    public void setUrbanPopulation(int urbanPop) {
        this.urbanPopulation = urbanPop;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getWealth() {
        return (Double) underlyingNode.getProperty("wealth");
    }

    public void setWealth(double wealth) {
        this.wealth = wealth;
    }

    public Node getPopulationFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(SIM_POPULATION)) {
            int during = Integer.parseInt(r.getProperty("year").toString());
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public Node getUrbanPopFact(int year) {
        for (Relationship r : underlyingNode.getRelationships(SIM_URBAN_POPULATION)) {
            int during = Integer.parseInt(r.getProperty("year").toString());
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public void abuts(Tile otherTile) {
        if (!this.equals( otherTile) ) {
            Relationship r = getAbuts(otherTile);
            if (r == null) {
                underlyingNode.createRelationshipTo(otherTile.getUnderlyingNode(), ABUTS);
            }
        }
    }

    public void abutsAddress(String a) {
        if (this.getAddress() !=a ) {
            Tile other = new Tile (db.findNode(Labels.Tile, "address", a) );
            if (getAbuts(other) == null) {
                abuts(other);
            }
        }
    }

    private Relationship getAbuts(Tile tile) {
        Node other = tile.getUnderlyingNode();
        for (Relationship rel : underlyingNode.getRelationships(ABUTS)) {
            if (rel.getOtherNode(underlyingNode).equals(other) ) {
                return rel;
            }
        }
        return null;
    }

    public Relationship getPopFactRelation(int year) {
        for (Relationship rel : underlyingNode.getRelationships(SIM_POPULATION)) {
            if ((Integer) rel.getProperty("year") == year) {
                return rel;
            }
        }
        return null;
    }

    public Relationship getUrbanPopFactRelation(int year) {
        for (Relationship rel : underlyingNode.getRelationships(SIM_URBAN_POPULATION)) {
            if ((Integer) rel.getProperty("year") == year) {
                return rel;
            }
        }
        return null;
    }

    public Relationship getFactDuringRel(Node y) {
        for (Relationship rel : underlyingNode.getRelationships(DURING)) {
            if (rel.getOtherNode(underlyingNode).equals(y)) {
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
        return o instanceof Tile && underlyingNode.equals( ((Tile) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Tile[" + getAddress() + "]";
    }



}

