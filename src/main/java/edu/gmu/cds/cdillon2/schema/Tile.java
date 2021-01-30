package edu.gmu.cds.cdillon2.schema;

import com.uber.h3core.H3Core;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import java.util.ArrayList;
import java.util.List;

import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;


public class Tile {
    private static final String ADDRESS = "address";
    private final Node underlyingNode;
    private Long h3Id;
    private int face;
    private int resolution;
    private double population = 0;
    private double urbanPopulation = 0;
    private double wealth = 0.0;
    private double gtp = 0.0; // gross tile product
    private double builtArea = 0.0;
    private double computedValue = 0.0;
    private int year;
    private int representation;

    @Context
    GraphDatabaseService db;

    public Tile(Node node) {
        this.underlyingNode = node;
    }

    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    public void configureTile() {
        try {
            H3Core h3 = H3Core.newInstance();
            String h3address = getAddress();
            this.h3Id = h3.stringToH3(h3address);
            this.face = h3.h3GetBaseCell(h3address);
            this.resolution = h3.h3GetResolution(h3address);
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
        List<String> neighbors = new ArrayList<>();
        String h3address = getAddress();
        try {
           H3Core h3 = H3Core.newInstance();
           neighbors = h3.hexRing(h3address, 1);
           neighbors.remove(h3address);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return neighbors;
    }

    public double getPopulation() {
        return population;
    }

    public void setPopulation(double pop) {
        this.population = pop;
    }

    public void addPopulation(double p) {
        this.population += p;
    }

    public Node getPopulationFact(int year) {
        RelationshipTypes popRel = SIM_POPULATION_1816;

        switch (year) {
            case 1816:
                popRel = SIM_POPULATION_1816;
                break;
            case 1850:
                popRel = SIM_POPULATION_1850;
                break;
            case 1880:
                popRel = SIM_POPULATION_1880;
                break;
            case 1914:
                popRel = SIM_POPULATION_1914;
                break;
            case 1938:
                popRel = SIM_POPULATION_1938;
                break;
            case 1945:
                popRel = SIM_POPULATION_1945;
                break;
            case 1994:
                popRel = SIM_POPULATION_1994;
                break;
        }
        for (Relationship r : underlyingNode.getRelationships(popRel)) {
            int rl = ((Number) r.getProperty("during")).intValue();
            if (rl==year) {
                return r.getOtherNode(underlyingNode);
            }
        }
        return null;
    }

    public Double getPopulationFactValue(int y) {
        Node f = getPopulationFact(y);
        Double iv = (Double) f.getProperty("value");
        return iv;
    }

    public double getUrbanPopulation() {
        return urbanPopulation;
    }

    public void setUrbanPopulation(double urbanPop) {
        this.urbanPopulation = urbanPop;
    }

    public Node getUrbanPopFact(int year) {
        RelationshipTypes uPopRel = SIM_URBAN_POPULATION_1816;

        switch (year) {
            case 1816:
                uPopRel = SIM_URBAN_POPULATION_1816;
                break;
            case 1850:
                uPopRel = SIM_URBAN_POPULATION_1850;
                break;
            case 1880:
                uPopRel = SIM_URBAN_POPULATION_1880;
                break;
            case 1914:
                uPopRel = SIM_URBAN_POPULATION_1914;
                break;
            case 1938:
                uPopRel = SIM_URBAN_POPULATION_1938;
                break;
            case 1945:
                uPopRel = SIM_URBAN_POPULATION_1945;
                break;
            case 1994:
                uPopRel = SIM_URBAN_POPULATION_1994;
                break;
        }
        for (Relationship r : underlyingNode.getRelationships(uPopRel)) {
            int during = ((Number) r.getProperty("during")).intValue();
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public Double getUrbanPopFactValue(int year) {
        Node f = getPopulationFact(year);
        Double iv = (Double) f.getProperty("value");
        return iv;
    }

    public double getWealth() {
        return wealth;
    }

    public void setWealth(double wealth) {
        this.wealth = wealth;
    }

    public Node getWealthFact(int year) {
        RelationshipTypes wRel = SIM_WEALTH_1816;

        switch (year) {
            case 1816:
                wRel = SIM_WEALTH_1816;
                break;
            case 1850:
                wRel = SIM_WEALTH_1850;
                break;
            case 1880:
                wRel = SIM_WEALTH_1880;
                break;
            case 1914:
                wRel = SIM_WEALTH_1914;
                break;
            case 1938:
                wRel = SIM_WEALTH_1938;
                break;
            case 1945:
                wRel = SIM_WEALTH_1945;
                break;
            case 1994:
                wRel = SIM_WEALTH_1994;
                break;
        }
        for (Relationship r : underlyingNode.getRelationships(wRel)) {
            int during = ((Number) r.getProperty("during")).intValue();
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public Double getWealthFactValue(int year) {
        Node f = this.getWealthFact(year);
        Double iv = (Double) f.getProperty("value");
        return iv;
    }

    public Node getGTPFact(int year) {
        RelationshipTypes gtpRel = SIM_PRODUCTION_1816;

        switch (year) {
            case 1816:
                gtpRel = SIM_PRODUCTION_1816;
                break;
            case 1850:
                gtpRel = SIM_PRODUCTION_1850;
                break;
            case 1880:
                gtpRel = SIM_PRODUCTION_1880;
                break;
            case 1914:
                gtpRel = SIM_PRODUCTION_1914;
                break;
            case 1938:
                gtpRel = SIM_PRODUCTION_1938;
                break;
            case 1945:
                gtpRel = SIM_PRODUCTION_1945;
                break;
            case 1994:
                gtpRel = SIM_PRODUCTION_1994;
                break;
        }
        for (Relationship r : underlyingNode.getRelationships(gtpRel)) {
            int during = ((Number) r.getProperty("during")).intValue();
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public Double getGTPFactValue(int year) {
        Node f = getGTPFact(year);
        Double iv = (Double) f.getProperty("value");
        return iv;
    }

    public double getGtp() {
        return gtp;
    }

    public void setGtp(double gtp) {
        this.gtp = gtp;
    }

    public double getBuiltArea() {
        return builtArea;
    }

    public void setBuiltArea(double builtArea) {
        this.builtArea = builtArea;
    }

    public Double getBuiltAreaFactValue(int year) {
        Node f = getBuiltAreaFact(year);
        Double ba = (Double) f.getProperty("value");
        return ba;
    }

    public Node getBuiltAreaFact(int year) {
        RelationshipTypes builtRel = SIM_BUILT_AREA_1816;
        switch (year) {
            case 1816:
                builtRel = SIM_BUILT_AREA_1816;
                break;
            case 1850:
                builtRel = SIM_BUILT_AREA_1850;
                break;
            case 1880:
                builtRel = SIM_BUILT_AREA_1880;
                break;
            case 1914:
                builtRel = SIM_BUILT_AREA_1914;
                break;
            case 1938:
                builtRel = SIM_BUILT_AREA_1938;
                break;
            case 1945:
                builtRel = SIM_BUILT_AREA_1945;
                break;
            case 1994:
                builtRel = SIM_BUILT_AREA_1994;
                break;
        }
        for (Relationship r : underlyingNode.getRelationships(builtRel)) {
            int during = ((Number) r.getProperty("during")).intValue();
            if (during==year) {
                return r.getOtherNode(this.underlyingNode);
            }
        }
        return null;
    }

    public double getComputedValue(String p) {
        if (p == "builtArea") {
            return computedValue / 1770.3235517;
        } else {
            return computedValue;
        }
    }

    public void setComputedValue(double c) {
        this.computedValue = c;
    }

    public void addComputedValue(double c) {
        computedValue += c;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void abuts(Tile otherTile) {
        if (!this.equals( otherTile) ) {
            try {
                getAbuts(otherTile);
            } catch (NotFoundException e) {
                return;
            }
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

    public boolean isWater() {
        try {
            return (boolean) getUnderlyingNode().getProperty("isWater");
        } catch (NotFoundException e) {
            return false;
        }
    }

    public Relationship getFactDuringRel(Node y) {
        for (Relationship rel : underlyingNode.getRelationships(DURING)) {
            try {
                rel.getOtherNode(underlyingNode);
            } catch (NotFoundException e) {
                return null;
            }
            if (rel.getOtherNode(underlyingNode).equals(y)) {
                return rel;
            }
        }
        return null;
    }

    public int getRepresentation() {
        return representation;
    }

    public void setRepresentation(int representation) {
        this.representation = representation;
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

