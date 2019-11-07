package edu.gmu.cds.cdillon2.schema;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.geojson.Feature;
import org.geojson.LngLatAlt;
import org.geojson.MultiPolygon;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.*;

import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.INCLUDES;
import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.OCCUPIED;


public class Territory {
    private static final String MAPKEY = "mapKey";
    private final Node underlyingNode;
    private Feature feature;
    private String abbr;
    private String name;
    private String cowcode;
    private int year;
    private int resolution;
    private double area;
    private List<String> tiles;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;


    public Territory (Node node) {
        this.underlyingNode = node;
    }

    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    public Node configureTerritory(Feature f, int y, int r) {
        int cowInt = (f.getProperty("CCODE") != null) ? f.getProperty("CCODE") : 0;
        this.year = y;
        this.feature = f;
        this.resolution = (r > 0 && r <= 15) ? r : 4;
        this.name = feature.getProperty("NAME");
        this.abbr = feature.getProperty("WB_CNTRY") != null ? feature.getProperty("WB_CNTRY") : "UNK";
        this.cowcode = (cowInt > 0) ? "" + cowInt : "NA";
        this.area = feature.getProperty("AREA") != null ? feature.getProperty("AREA") : 0.0;
        this.tiles = calculatePolyfill(feature, resolution);
        underlyingNode.setProperty("year", year);
        underlyingNode.setProperty("name", name);
        underlyingNode.setProperty("resolution", resolution);
        underlyingNode.setProperty("abbr", abbr);
        underlyingNode.setProperty("cowcode", cowcode);
        underlyingNode.setProperty("area", area);
        return underlyingNode;
    }

    private List<String> calculatePolyfill(Feature f, int r) {
        List<String> addresses = new ArrayList<>();

        MultiPolygon geom = (MultiPolygon) f.getGeometry();
        int numPolygons = geom.getCoordinates().size();
        int goalRes = r;
        int tempRes = goalRes + 1;

        Set<String> tempList = new HashSet<>();
        List<String> linkedTileIAddresses = new ArrayList<>();

        for (int i = 0; i < numPolygons; i++) {
            List<List<GeoCoord>> holes = new ArrayList<>();
            int numInnerLists = geom.getCoordinates().get(i).size();

            List<LngLatAlt> coordinates = geom.getCoordinates().get(i).get(0);
            List<GeoCoord> boundaryCoordinates = swapCoordinateOrdering(coordinates);

            if (numInnerLists > 1) {        // second thru last elements are holes in the outer polygon
                for (int il=1; il<numInnerLists; il++) {
                    List<GeoCoord> hole = swapCoordinateOrdering(geom.getCoordinates().get(i).get(il));
                    holes.add(hole);
                }
            }

            try {
                H3Core h3 = H3Core.newInstance();
                tempList.addAll(h3.polyfillAddress(boundaryCoordinates, holes, tempRes));
                for (String t : tempList) {
                    String tParent = h3.h3ToParentAddress(t, goalRes);
                    List<String> tSiblings = h3.h3ToChildren(tParent, tempRes);
                    if (tempList.contains(tSiblings.get(0))) {
                        linkedTileIAddresses.add(tParent);
                    }
                }
            } catch (Exception e) {
                log.debug("Territory.class makeTiles() method is the culprit.");
                e.printStackTrace();
            }
        }

        return linkedTileIAddresses;
    }

    private List<GeoCoord> swapCoordinateOrdering(@NotNull List<LngLatAlt> coordinates) {
        List<GeoCoord> h3Coords = new ArrayList<>();
        for (LngLatAlt c : coordinates) {
            GeoCoord gc = new GeoCoord(c.getLatitude(), c.getLongitude());
            h3Coords.add(gc);
        }
        return h3Coords;
    }

    public int getYear() {
        return Integer.parseInt(underlyingNode.getProperty("year").toString());
    }

    public Feature getFeature() {
        return feature;
    }

    public List<String> getTiles() {
        return tiles;
    }

    public String getMapkey() {
        return (String) underlyingNode.getProperty(MAPKEY);
    }

    public String getAbbr() {
        return (String) underlyingNode.getProperty("abbr");
    }

    public String getName() {
        return (String) underlyingNode.getProperty("name");
    }

    public String getCowcode() {
        return (String) underlyingNode.getProperty("cowcode");
    }

    public int getResolution() {
        return (Integer) underlyingNode.getProperty("resolution");
    }

    public double getArea() {
        return (Double) underlyingNode.getProperty("area");
    }

    public List<Node> getIncludedTileNodes() {
        List<Node> included = new ArrayList<>();
        for (Relationship r : underlyingNode.getRelationships(INCLUDES)) {
            included.add(r.getOtherNode(underlyingNode));
        }
        return included;
    }

    public Map<String, Tile> convertTileLinks(Map<String, Tile> map) {
        Map<String, Tile> occupiedTiles = new HashMap<>();
        try (Transaction tx = db.beginTx()) {
            for (String a : tiles) {
                Node n = db.findNode(Labels.Tile, "address", a);
                Tile t;
                if (n==null) {
                    n = db.createNode(Labels.Tile);
                    n.setProperty("address", a);
                    t = new Tile(n);
                    t.configureTile();
                } else {
                    t = map.get(a);
                }
                Relationship r = this.underlyingNode.createRelationshipTo(
                        t.getUnderlyingNode(), RelationshipTypes.INCLUDES);
                r.setProperty("year", year);
                occupiedTiles.put(a, t);
            }
            tx.success();
        }
        return occupiedTiles;
    }

    public Relationship includes(Tile tile) {
        Relationship r = getIncludes(tile);
        if (r == null) {
            r = underlyingNode.createRelationshipTo(tile.getUnderlyingNode(), RelationshipTypes.INCLUDES);
        }
        return r;
    }

    public Relationship getIncludes(Tile tile) {
        Node t = tile.getUnderlyingNode();
        for (Relationship r : t.getRelationships(RelationshipTypes.INCLUDES) ) {
            if (r.getOtherNode(underlyingNode).equals(t)) {
                return r;
            }
        }
        return null;
    }

    public Polity getPolity() {
        Node p = underlyingNode.getSingleRelationship(OCCUPIED, Direction.INCOMING).getOtherNode(underlyingNode);
        return new Polity(p);
    }

    @Override
    public int hashCode() {
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Territory && underlyingNode.equals( ((Territory) o).getUnderlyingNode() );
    }

    @Override
    public String toString() {
        return "Territory[" + getMapkey() + "]";
    }




}
