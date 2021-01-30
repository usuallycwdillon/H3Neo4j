package edu.gmu.cds.cdillon2.wog;

import edu.gmu.cds.cdillon2.schema.Dataset;
import edu.gmu.cds.cdillon2.schema.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GeoDatasetData {

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    public static Map<Integer, Dataset> GEODATASETS;


    static Dataset map1816 = new Dataset(
            "cowWorld_1816.geojson",
            "world 1816",
            0.9,
            1816);

    static Dataset map1880 = new Dataset(
            "cowWorld_1880.geojson",
            "world 1880",
            0.9,
            1880);

    static Dataset map1914 = new Dataset(
            "cowWorld_1914.geojson",
            "world 1914",
            0.9,
            1914);

    static Dataset map1938 = new Dataset(
            "cowWorld_1938.geojson",
            "world 1938",
            0.9,
            1938);

    static Dataset map1945 = new Dataset(
            "cowWorld_1945.geojson",
            "world 1945",
            0.9,
            1945);

    static Dataset map1994 = new Dataset(
            "cowWorld_1994.geojson",
            "world 1994",
            0.9,
            1994);

    static {
        Map<Integer, Dataset> mapdata = new HashMap<>();
        mapdata.put(1816, map1816);
        mapdata.put(1880, map1880);
        mapdata.put(1914, map1914);
        mapdata.put(1938, map1938);
        mapdata.put(1945, map1945);
        mapdata.put(1994, map1994);
        GEODATASETS = Collections.unmodifiableMap(mapdata);
    }

    private Node createDatasetNode(String filename, String name, Double version, int year) {
        Node underlyingNode;
        try (Transaction tx = db.beginTx()) {
            Node n = db.findNode(Labels.Dataset,"name", name);
            if (n==null) {
                n = db.createNode(Labels.Dataset);
                n.setProperty("name", name);
                n.setProperty("filename", filename);
                n.setProperty("version", version);
                n.setProperty("year", year);
            }
            underlyingNode = n;
            tx.success();
        }
        return underlyingNode;
    }



}
