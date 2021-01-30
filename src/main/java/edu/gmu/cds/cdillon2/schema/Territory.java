package edu.gmu.cds.cdillon2.schema;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import ec.util.MersenneTwisterFast;
import edu.gmu.cds.cdillon2.util.MTFApache;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.geojson.Feature;
import org.geojson.LngLatAlt;
import org.geojson.MultiPolygon;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import sim.util.distribution.Distributions;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;


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
    private double pop;
    private double totalPop;
    private double uPop;
    private double totalUpop;
    private double milex;
    private double wealth;
    private double totalWealth;
    private double popRatio = 0.0;
    private double uPopRatio = 0.0;
    private double wealthRatio = 0.0;
    private List<String> tiles;
    private List<Long> tileIds;
    private Map<String, Tile> tileMap;
    public Map<String, Tile> outputMap = new HashMap<>();
    private Tile loneTile;
    private Map<Tile, Double> sortedTileMap = new LinkedHashMap<>();
    private MersenneTwisterFast random;
    private String message;

    @Context
    GraphDatabaseService db;

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
        String sy = underlyingNode.getProperty("year").toString();
        Long ly = (Long) underlyingNode.getProperty("year");
        Integer iy = ly == null ? null : Math.toIntExact(ly);
        return iy;
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
        Long res = (Long) underlyingNode.getProperty("resolution");
        Integer r = res == null ? null : Math.toIntExact(res);
        return r;
    }

    public Long[] getTileIds() {
        return (Long[]) underlyingNode.getProperty("linkedTileIds");
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

    public Map<String, Tile> getTileMap() {
        Map<String, Tile> tileMap = new HashMap<>();
        for (Relationship r : underlyingNode.getRelationships(INCLUDES)) {
            Tile t = new Tile(r.getOtherNode(underlyingNode));
            String a = t.getAddress();
            tileMap.put(a, t);
        }
        return tileMap;
    }

    public List<Node> getTileNodeList() {
        List<Node> tiles = new ArrayList<>();
        for(Relationship r : underlyingNode.getRelationships(INCLUDES)) {
            tiles.add(r.getOtherNode(underlyingNode));
        }
        return tiles;
    }

    public Map<String, Tile> getOutputMap() {
        return this.outputMap;
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
                        t.getUnderlyingNode(), INCLUDES);
                r.setProperty("during", year);
                occupiedTiles.put(a, t);
            }
            tx.success();
        }
        return occupiedTiles;
    }

    public String getMessage() {
        return this.message;
    }

    public Tile getLoneTile() {
        return this.loneTile;
    }

    public Relationship includes(Tile tile) {
        Relationship r = getIncludes(tile);
        if (r == null) {
            r = underlyingNode.createRelationshipTo(tile.getUnderlyingNode(), INCLUDES);
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

    public Polity findPolity() {
        Relationship r = underlyingNode.getSingleRelationship(OCCUPIED, Direction.INCOMING);
        Node up;
        try {
            up = r.getOtherNode(underlyingNode);
        } catch (NullPointerException e) {
            return null;
        }
        if (up != null) {
            Polity p = new Polity(up);
            return p;
        }
        return null;
    }

    public Node findYearNode() {
        Relationship r = underlyingNode.getSingleRelationship(DURING, Direction.OUTGOING);
        Node yN = r.getOtherNode(this.underlyingNode);
        return yN;
    }

    public void setPop(int p) {
        this.pop = p;
    }

    public void setUPop(int u) {
        this.uPop = u;
    }

    public void setMilex(int m) {
        this.milex = m;
    }

    public Territory calculateTileFacts(Log l) {
        log = l;
        this.message = this.getMapkey();
        random = new MersenneTwisterFast();
        tileMap = getTileMap();
        Polity p = findPolity() != null ? findPolity() : null;
        if (p == null) {
            log.info(message + " has no Polity.");
            message += " has no Polity for which to collect data.";
            return this;
        }
        int yearVal = getYear();
        this.pop = p.getPopFact(yearVal);
        this.uPop = p.getUrbanPopFact(yearVal);
        this.milex = p.getMilExFact(yearVal);
        this.wealth = milex * 250.0;               // WARNING: This is a mostly made-up number
        this.totalPop = 0;
        this.totalUpop = 0;
        this.totalWealth = 0.0;
        log.info(message + " has pop of " + pop + ", urban pop of " + uPop + ", and wealth of " + wealth);

        // Three conditions: there are multiple tiles, a single tile, no tiles
        if (tileMap.size() > 1) {
            // Simulate the population of tiles and attribute to Tile objects
            if (pop > 0 && computeSimulatedTilePops()) {
                message += " has pop " + totalPop + " (" + popRatio + ")\n";
            }
            // Sort the populated Tiles into new LinkedHashMap
            sortedTileMap = sortedTileMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (x0, x1) -> {
                                throw new AssertionError();
                            },
                            LinkedHashMap::new
                    ));
            // Simulate the urban population on tiles with adequate populations
            if (uPop > 0 && computeSimulatedTileUrbanPops()) {
                message += ", has urban pop " + totalUpop + " (" + uPopRatio + ")\n";
            } else {
                message += " has no urban pop";
            }
            // Simulate tile wealth, ranked by populations
            if (wealth > 0 && computeSimulatedTileWealth()) {
                message += " and, has wealth " + totalWealth + " (" + wealthRatio + " of MILEX)";
            } else {
                message += " but, has no wealth.";
            }
//            log.info(message + " has finished computing tile facts and is about to save to the database. ");
            return this;
        } else if (tileMap.size() == 1) {
            // If there's only one tile, it gets all of the Territory's attribute values
            loneTile = tileMap.values().iterator().next();
            loneTile.setPopulation(pop * 1.000);
            loneTile.setUrbanPopulation(uPop * 1.000);
            loneTile.setWealth(wealth * 1.000);
            sortedTileMap.put(loneTile, pop);
            popRatio = 1.0;
            uPopRatio = 1.0;
            wealthRatio = 1.0;
            message += " has a single tile with all Territory attribute values.";
//            log.info(message);
            return this;
        } else {
            message += "ignore";
            return this;
        }
    }

    private boolean computeSimulatedTilePops() {
        totalPop = 0;
        int num = tileMap.size();

        // TODO: explain this exponent
        double pl  = Distributions.nextPowLaw(-1.15, 0.1, random);
        ZipfDistribution zd = new ZipfDistribution(new MTFApache(random), num, 1.06);
        int[] clevels = zd.sample(num);
        int[] levels = new int[clevels.length];
        // some tiles should have 0 population rather than have fewer than 100 people
        for (int i=0;i<levels.length; i++) {
            levels[i] = levels[i] -1;
        }
        Integer[] iLevels = Arrays.stream(levels).boxed().toArray( Integer[]::new );
        int levelSum = IntStream.of(levels).sum();
        Arrays.sort(iLevels, Collections.reverseOrder());
        double proportion = (pop * 1.0) / levelSum;

        int i = 0;
        for (Map.Entry<String, Tile> e : tileMap.entrySet()) {
            if (i < num) {
                double val = (double) (iLevels[i] * proportion * 1000);
                Tile tile = e.getValue();
                tile.setPopulation(val);
                sortedTileMap.put(tile, val);
                String add = tile.getAddress();
                outputMap.put(add, tile);
                totalPop += val;
                i++;
//                log.info(add + " has pop " + val);
            }
        }

        popRatio = (totalPop * 1.0) / pop;
        return true;
    }

    private boolean computeSimulatedTileUrbanPops() {
        totalUpop = 0;
        uPopRatio = 0.0;
        double uPopAccounting = uPop;
        Tile firstTile = sortedTileMap.keySet().iterator().next();

        int div = (int) Math.round(uPop / 50);
        Integer uDiv = div > 1 ? div - 1 : 1;
        String binary = Integer.toBinaryString(uDiv);
        int num = binary.length();

        // This exponent is Zipf's original exp and is close to T. Gulden etAl's 0.98 using urban lights
        ZipfDistribution zd = new ZipfDistribution(new MTFApache(random), num, 1.0);
        int[] levels = zd.sample(num);
        Integer[] iLevels = Arrays.stream(levels).boxed().toArray( Integer[]::new );
        int levelSum = IntStream.of(levels).sum();
        Arrays.sort(iLevels, Collections.reverseOrder());
        double proportion = (uPop * 1.0) / levelSum;

        int i = 0;
        for (Map.Entry<Tile, Double> e : sortedTileMap.entrySet()) {
            if (i < num) {
                double val = (double) (iLevels[i] * proportion * 1000);
                Tile tile = e.getKey();
                // It is sometimes necessary to adjust the tile pop when the simulated urban pop is bigger.
                if (val <= e.getValue()) {
                    tile.setUrbanPopulation(val);
                } else {
                    tile.setUrbanPopulation(val);
                    tile.setPopulation(val);
                }
                totalUpop += val;
                uPopAccounting -= val;
                outputMap.put(tile.getAddress(), tile);
                i++;
            }
        }

        if (uPopAccounting > 0) {
            double oldUPop = firstTile.getUrbanPopulation();
            double newUPop = oldUPop + uPopAccounting;
            double oldPop = firstTile.getPopulation();
            firstTile.setUrbanPopulation(newUPop);
            if (oldPop < newUPop) {
                firstTile.setPopulation(newUPop);
                outputMap.put(firstTile.getAddress(), firstTile);
            }
        }

        uPopRatio = (totalUpop * 1.0) / uPop;
        return true;
    }

    public boolean computeSimulatedTileWealth() {
        totalWealth = 0.0;
        LinkedHashMap<Tile, Double> popdSortedTileMap = sortedTileMap.entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new) );

        int num = popdSortedTileMap.size();

        // This exponent is 1.36 according to M. Levy and S. Solomon (1997), Physica A: New evidence of the power law
        // distribution in wealth.  https://www.sciencedirect.com/science/article/pii/S0378437197002173
        // I order the ranking by tile population
        ZipfDistribution zd = new ZipfDistribution(new MTFApache(random), num, 1.36);
        int[] levels = zd.sample(num);
        Integer[] iLevels = Arrays.stream(levels).boxed().toArray(Integer[]::new);
        int levelSum = IntStream.of(levels).sum();
        Arrays.sort(iLevels, Collections.reverseOrder());
        double proportion = (wealth * 1.0) / levelSum;

        int i = 0;
        for (Map.Entry<Tile, Double> e : popdSortedTileMap.entrySet()) {
            if (i < num) {
                int val = (int) (iLevels[i] * proportion * 1000);
                Tile tile = e.getKey();
                tile.setWealth(val);
                totalWealth += val;
                outputMap.put(tile.getAddress(), tile);
                i++;
            }
        }
        wealthRatio = (totalWealth * 1.0) / (milex * 1000);
        return true;
    }

    public boolean orderTilesXdescendingPopulation(String propType, int yr) {
        List<Node> tileNodes = getIncludedTileNodes();
        for (Node n : tileNodes) {
            Tile t = new Tile(n);
            double f = t.getPopulationFactValue(yr);
            sortedTileMap.put(t, f);
        }

        sortedTileMap = sortedTileMap.entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (x0, x1) -> {
                            throw new AssertionError();
                        },
                        LinkedHashMap::new
                ));
        return true;
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
