package edu.gmu.cds.cdillon2.wog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.util.GeoCoord;
import edu.gmu.cds.cdillon2.schema.*;
import edu.gmu.cds.cdillon2.schema.Year;
import edu.gmu.cds.cdillon2.util.MTFApache;
import edu.gmu.cds.cdillon2.util.MTFWrapper;
import edu.gmu.cds.cdillon2.results.*;
import static edu.gmu.cds.cdillon2.schema.Labels.*;
import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;
import org.apache.commons.math3.distribution.ZipfDistribution;
import com.uber.h3core.H3Core;
import ec.util.MersenneTwisterFast;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.bouncycastle.math.raw.Mod;
import org.geojson.*;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.alg.scoring.BetweennessCentrality;
import org.jgrapht.generate.WattsStrogatzGraphGenerator;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.util.SupplierUtil;
import org.neo4j.cypher.internal.frontend.v2_3.ast.Return;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Has;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;


import java.io.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.time.*;

/**
 *
 *
 */
public class Wog {

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;


    @Procedure(name = "wog.returnHexAddress", mode = Mode.WRITE)
    @Description("Call wog.returnHexAddress(latitude, longitude, resolution")
    /**
     * Copied from David Fauth
     */
    public Stream<StringResult> getH3Address(
            @Name("locLatitude") Double locLatitude,
            @Name("locLongitude") Double locLongitude,
            @Name("resolution") String resolution) throws InterruptedException {
        String address = null;

        try {
            H3Core h3 = H3Core.newInstance();
            int newRes = Integer.parseInt(resolution);
            int res = (Integer.parseInt(resolution) > 0 && Integer.parseInt(resolution) <= 15) ? Integer.parseInt(resolution) : 4;
            address = h3.geoToH3Address(locLatitude, locLongitude, res);
        } catch (Exception e) {
            System.out.println(e);
        }

        return Stream.of(new StringResult(address));
    }

    @Procedure(name = "wog.returnTileNode", mode = Mode.WRITE)
    @Description("Call wog.returnHexTile(latitude, longitude, resolution")
    public Stream<NodeResult> getH3TileNode(
            @Name("locLatitude") Double locLatitude,
            @Name("locLongitude") Double locLongitude,
            @Name("resolution") String resolution) throws InterruptedException {

        Node node = null;

        try {
            H3Core h3 = H3Core.newInstance();
            int pRes = Integer.parseInt(resolution);
            int res = (pRes > 0 && pRes <= 15) ? pRes : 4;

            String address = h3.geoToH3Address(locLatitude, locLongitude, res);
            Long h3Id = h3.geoToH3(locLatitude, locLongitude, res);
            Integer face = h3.h3GetBaseCell(h3Id);

            node = db.createNode(Tile);
            node.setProperty("address", address);
            node.setProperty("h3Id", h3Id);
            node.setProperty("face", face);
            node.setProperty("resolution", res);

        } catch (Exception e) {
            System.out.println(e);
        }

        return Stream.of(new NodeResult(node));
    }

    @Procedure(name = "wog.returnHexArea", mode = Mode.READ)
    @Description("Call wog.returnHexArea(address)")
    /**
     *
     */
    public Stream<DoubleResult> getH3Area(
            @Name("address") String address)
            throws InterruptedException {
        String add = address;
        Double area = 0.0;
        try {
            H3Core h3 = H3Core.newInstance();
            area = h3.hexArea(4, AreaUnit.km2);
        } catch (Exception e) {
            log.info("H3 could not get the area for " + add);
        }

        return Stream.of(new DoubleResult(area));
    }

    @Procedure(name = "wog.polyfillFeaturesFile", mode = Mode.WRITE)
    @Description("Call wog.polyfillFeaturesFile(filename, year, resolution)")
    public Stream<StringResult> polyfillFeaturesFile(
            @Name("filename") String filename,
            @Name("year") String year,
            @Name("resolution") String resolution
    ) throws InterruptedException {

        log.debug("Initiating polyfillFeaturesFile procedure");
        Map<String, Territory> territories = new HashMap<>();
        Map<String, edu.gmu.cds.cdillon2.schema.Tile> tiles = new HashMap<>();
        String result;
        Node d;

        int res = Integer.parseInt(resolution);
        int yr = Integer.parseInt(year);
        log.debug("Procedure initialized...");

        try (Transaction tx = db.beginTx()) {
            String name = "world " + year;
            d = db.createNode(Dataset);
            d.setProperty("filename", filename);
            d.setProperty("name", name);
            d.setProperty("version", 0.9);
            tx.success();
            log.debug("Dataset node created");
        }

        // Parse the geojson file into a list of features, but exclude Antarctica because it's politically useless and giant
        List<Feature> features = geoJsonProcessor(filename);

        for (Feature f : features) {
            if (!f.getProperty("NAME").equals("Antarctica")) {
                String key = f.getProperty("NAME") + " of " + year;
                Node t = db.createNode(Territory);
                t.setProperty("mapKey", key);
                Territory tt = new Territory(t);
                tt.configureTerritory(f, yr, res);
                territories.put(key, tt);
            }
        }

        log.debug("Features (minus Antarctica) are converted into Territory objects");

        territories = purgeTinyTerritories(territories);

        // Turn each Territory object into a graph of (:Dataset)-[:PROVIDES]->(:Territory)
        for (Territory t : territories.values()) {
            log.debug("Tile nodes have been successfully created.");
            try (Transaction tx = db.beginTx()) {
                d.createRelationshipTo(t.getUnderlyingNode(), PROVIDES);
                log.debug("Dataset now linked to a territory node");
                tx.success();
            } catch (Exception e) {
                log.debug("Creating the graphs in the main loop is the culprit.");
                e.printStackTrace();
            }
        }

        for (Territory t : territories.values()) {
            tiles.putAll(t.convertTileLinks(tiles));
        }

        String size = "" + territories.size();
        result = "Created " + size + " territories";
        return Stream.of(new StringResult(result));
    }

    @Procedure(name = "wog.polyfillStoredData", mode = Mode.WRITE)
    @Description("Call wog.polyfillStoredData(String H3 resolution")
    public Stream<StringResult> polyfillStoredData(
            @Name("resolution") String resolution) throws InterruptedException {
        String result = "Not successfull";

        Map<Integer, String> geodata = new HashMap<>();
        geodata.put(1816, "cowWorld_1816.geojson");
        geodata.put(1880, "cowWorld_1880.geojson");
        geodata.put(1914, "cowWorld_1914.geojson");
        geodata.put(1938, "cowWorld_1938.geojson");
        geodata.put(1945, "cowWorld_1945.geojson");
        geodata.put(1994, "cowWorld_1994.geojson");
        log.debug("The geodata map has been initiated...");

        Map<String, Node> territories = new HashMap<>();
        Map<String, Node> tiles = new HashMap<>();
        Map<String, Node> missingTiles = new HashMap<>();

        for (Map.Entry entry : geodata.entrySet()) {
            int year = (int) entry.getKey();
            String yr = year + "";
            String fn = entry.getValue() + "";
            polyfillGeoDataFiles(fn, yr, resolution);
            result = "Completed " + fn;
        }

        return Stream.of(new StringResult(result));
    }

    @Procedure(name = "wog.polyfillFeatureCollection", mode = Mode.WRITE)
    @Description("Call wog.polyfillFeatureCollection(String filename, String year, String H3 resolution)")
    public Stream<StringResult> polyfillGeoDataFiles(
            @Name("file") String file,
            @Name("year") String year,
            @Name("res") String res
    ) throws InterruptedException {

        log.debug("Initiating polyfillStoredGeoData procedure");
        Map<String, Territory> territories = new HashMap<>();
        Map<String, Tile> tiles = new HashMap<>();
        Map<String, Tile> missingTiles = new HashMap<>();

        // Loop through each entry in the geodatasets object to upload the appropriate geojson file, create territories,
        // and connect those territories to the polyfill tiles.
        int yr = Integer.parseInt(year);
        String filename = file;
        int resolution = Integer.parseInt(res);

        log.debug("Attempting to read the geojson file...");
        List<Feature> features = geoJsonProcessor(filename);

        // Identify the dataset node to which these territories will belong.
        Node d;
        try (Transaction tx = db.beginTx()) {
            d = db.findNode(Dataset, "filename", filename);
            tx.success();
        }

        // For each feature in the features collection:
        //  - create a key,
        //  - create a territory node and identify it by the unique key,
        //  - create a territory object for the node,
        //  - put the territory object into a map under its key
        //  - link the territory node to the aforementioned dataset node
        for (Feature f : features) {
            log.debug("Processing the features in the file. ");
            if (!f.getProperty("NAME").equals("Antarctica")) {
                String key = f.getProperty("NAME") + " of " + year;
                Territory t;
                try (Transaction tx = db.beginTx()) {
                    Node n = db.createNode(Territory);
                    n.setProperty("mapKey", key);
                    t = new Territory(n);
                    territories.put(key, t);
                    d.createRelationshipTo(n, PROVIDES);
                    tx.success();
                }
                t.configureTerritory(f, yr, resolution);
            }
        }
        territories = purgeTinyTerritories(territories);


        for (Territory t : territories.values()) {
            List<String> tileAddresses = t.getTiles();
            Node ter = t.getUnderlyingNode();
            try (Transaction tx = db.beginTx()) {
                for (String a : tileAddresses) {
                    Tile ti;
                    Node n = db.findNode(Labels.Tile, "address", a);
                    if (n == null) {
                        n = db.createNode(Labels.Tile);
                        n.setProperty("address", a);
                        ti = new Tile(n);
                        ti.configureTile();
                        n.setProperty("h3Id", ti.getH3Id());
                        n.setProperty("face", ti.getFace());
                        n.setProperty("resolution", ti.getResolution());
                    } else {
                        ti = tiles.get(a);
                    }
                    Relationship r = t.includes(ti);
                    r.setProperty("during", year);
                    tiles.put(a, ti);
                }
                tx.success();
            }
        }

        // Join the tiles together
        Iterator it = tiles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Tile t = (Tile) pair.getValue();
            for (String a : t.getNeighbors()) {
                if (tiles.containsKey(a)) {
                    Tile neighbor = tiles.get(a);
                    t.abuts(neighbor);
                } else {
                    Node newNode = db.createNode(Tile);
                    newNode.setProperty("address", a);
                    Tile newTile = new Tile(newNode);
                    newTile.configureTile();
                    missingTiles.put(a, newTile);
                    t.abuts(newTile);
                }
            }
        }

        if (missingTiles.size() != 0) {
            tiles.putAll(missingTiles);
        }

        for (Tile t : missingTiles.values()) {
            for (String a : t.getNeighbors()) {
                if (missingTiles.containsKey(a)) {
                    try (Transaction tx = db.beginTx()) {
//                        t.getUnderlyingNode().createRelationshipTo(missingTiles.get(a).getUnderlyingNode(), ABUTS);
                        t.abuts(missingTiles.get(a));
                        tx.success();
                    }
                }
            }
        }
        return Stream.of(new StringResult("Success " + filename) );
    }


    @Procedure(name = "wog.findDataset", mode = Mode.READ)
    @Description("Call wog.findDataset(String year)")
    public Stream<StringResult> findDataset( @Name("year") String year
    ) throws InterruptedException {
        int y = Integer.parseInt(year);
        String name;
        Dataset d;
        try (Transaction tx = db.beginTx()) {
            Node n = db.findNode(Dataset, "year", y);
            d = new Dataset(n);
            name = d.getName();
            tx.success();
        }
        return Stream.of(new StringResult(name));
    }


    @Procedure(name = "wog.getIncludedTilePop", mode = Mode.READ)
    @Description("Call wog.getIncludedTilePop(Node tile, String year)")
    public Stream<StringResult> getTilePop(
            @Name("tile") Node tile,
            @Name("year") String year
    ) throws InterruptedException {
        Tile t = new Tile(tile);
        int y = Integer.parseInt(year);
        Node f = t.getPopulationFact(y);
        Double v = (Double) f.getProperty("value");
        return Stream.of(new StringResult(v.toString()));
    }

    @Procedure(name = "wog.getIncludedTileUpop", mode = Mode.READ)
    @Description("Call wog.getIncludedTileUpop(Node tile, String year)")
    public Stream<StringResult> getTileuPop(
            @Name("tile") Node tile,
            @Name("year") String year
    ) throws InterruptedException {
        Tile t = new Tile(tile);
        int y = Integer.parseInt(year);
        Node f = t.getUrbanPopFact(y);
        Double v = (Double) f.getProperty("value");
        return Stream.of(new StringResult(v.toString()));
    }

    @Procedure(name = "wog.getIncludedTileBuiltArea", mode = Mode.READ)
    @Description("Call wog.getIncludedTileBuiltArea(Node tile, String year)")
    public Stream<StringResult> getTileBuiltArea(
            @Name("tile") Node tile,
            @Name("year") String year
    ) throws InterruptedException {
        Tile t = new Tile(tile);
        int y = Integer.parseInt(year);
        Node f = t.getBuiltAreaFact(y);
        Double v = (Double) f.getProperty("value");
        return Stream.of(new StringResult(v.toString()));
    }

    @Procedure(name = "wog.getIncludedTileProduction", mode = Mode.READ)
    @Description("Call wog.getIncludedTileProduction(Node tile, String year)")
    public Stream<StringResult> getTileProduction(
            @Name("tile") Node tile,
            @Name("year") String year
    ) throws InterruptedException {
        Tile t = new Tile(tile);
        int y = Integer.parseInt(year);
        Node f = t.getGTPFact(y);
        Double v = (Double) f.getProperty("value");
        return Stream.of(new StringResult(v.toString()));
    }

    @Procedure(name = "wog.getIncludedTileWealth", mode = Mode.READ)
    @Description("Call wog.getIncludedTileWealth(Node tile, String year)")
    public Stream<StringResult> getTileWealth(
            @Name("tile") Node tile,
            @Name("year") String year
    ) throws InterruptedException {
        Tile t = new Tile(tile);
        int y = Integer.parseInt(year);
        Node f = t.getWealthFact(y);
        Double v = (Double) f.getProperty("value");
        return Stream.of(new StringResult(v.toString()));
    }

    @Procedure(name = "wog.simulateTileFacts", mode = Mode.WRITE)
    @Description("CALL wog.simulateTileFacts(Node territory, Node computation)")
    public Stream<StringResult> generateTileData(
            @Name("territory") Node territory,
            @Name("computation") Node computation
    ) throws InterruptedException {
        Territory ter = new Territory(territory);
        String message = ter.getMapkey();
        log.info(message + " has been loaded...");
        Node y = ter.findYearNode();
        Map<String, Tile> tileMap;

        if (ter.getCowcode() != "NA") {
//            log.info(message + ". and it's tile facts are being computed and saved to the graph." );
            ter = ter.calculateTileFacts(log);
            tileMap = ter.getOutputMap();
            String voyer =  convertWithIteration(tileMap);
//            log.info(voyer);
            if (ter.getMessage() != "ignore" && tileMap.size() > 1) {
                try (Transaction tx = db.beginTx()) {
                    for (Tile e : tileMap.values()) {
                        saveTileFactsGraph(e, computation, y);
                    }
                    tx.success();
                }
                return Stream.of(new StringResult(ter.getMessage()));
            } else if (ter.getMessage() != "ignore" && tileMap.size() == 1) {
                Tile e = ter.getLoneTile();
                try (Transaction tx = db.beginTx())  {
                    saveTileFactsGraph(e, computation, y);
                    tx.success();
                }
                return Stream.of(new StringResult(ter.getMessage()));
            } else {
                return Stream.of(new StringResult("No data. Ignoring " + message));
            }
        } else {
            return Stream.of(new StringResult(message + " has no cowcode. Next!"));
        }
    }

    @Procedure(name = "wog.createCommonWeal", mode=Mode.WRITE)
    @Description(("CALL wog.createCommonWeal(Node territory, Long size, Long degree, Double probability"))
    public Stream<StringResult> createTerritorialSocialNetworks(
            @Name("territory") Node territory,
            @Name("size") Long size,
            @Name("degree") Long degree,
            @Name("probability") Double probability
    ) throws InterruptedException {
        // Collect pop data for this territory's tiles and make a sorted Map by pop
        final String mapKey = (String) territory.getProperty("mapKey");
        String message = mapKey;
        Territory t = new Territory(territory);
        int n = size.intValue();
        int d = degree.intValue();
        int y = t.getYear();
        Long yl = (long) y;
        double p = probability.doubleValue();
        Map<String, Person> personMap = new HashMap<>();
        Map<String, Integer> tileMap = new HashMap<>();
        Map<Person, String> personAddressMap = new HashMap<>();
        Deque<Person> personDeque = new LinkedList<>();
        Map<Tile, Integer> unSortedMap = new LinkedHashMap<>();
        Map<Tile, Integer> repCounts = new HashMap<>();
        log.info("Creating a CommonWeal for " + mapKey);

        Polity polity = t.findPolity() != null ? t.findPolity() : null;
        if (polity == null) {
            log.info(message + " has no Polity.");
            message += " has no Polity for which to collect data.";
        } else {
            // Build the Social Network Structure
//            log.info("Building a social network structure for " + mapKey);
            // JGraphT requires an implementation of Java's util.Random class. The MTWWrapper extends util.Random to
            // satisfy JGraphT, but actually uses MASON's MerseneTwisterFast.
            MTFWrapper random = new MTFWrapper(System.currentTimeMillis());
            WattsStrogatzGraphGenerator<String, DefaultEdge> wsg = new WattsStrogatzGraphGenerator<String, DefaultEdge> (
                    n, d, p, true, random);
            Supplier<String> vSupplier = new Supplier<String>() {
                private int id = 0;
                @Override
                public String get() {
                    String nodeId = "v" + id++;
                    return nodeId;
                }
            };

            Graph<String, DefaultEdge> localGraph = new SimpleGraph<>(
                    vSupplier, SupplierUtil.createDefaultEdgeSupplier(), false);
            wsg.generateGraph(localGraph);

//            log.info("The structure is built. Calculating betweenness centrality");
            BetweennessCentrality<String, DefaultEdge> bc = new BetweennessCentrality<>(localGraph);
            Map<String, Double> betweennessScores = bc.getScores();

            LinkedHashMap<String, Double> sortedBC = betweennessScores.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k, v) -> v, LinkedHashMap::new));

            Node polityFact = polity.getPolityFact(y);
            if (polityFact==null) {
                log.info(mapKey + " doesn't have a valid Polity Fact Node for " + y);
            }

            int aRating;
            try {
                aRating = polityFact.hasProperty("autocracyRating") ? ((Number) polityFact.getProperty("autocracyRating")).intValue() : 5 ;
                log.info(mapKey + " either has an autocracy rating or we just pretended it is 5");
            } catch (NullPointerException npe) {
                aRating = 5;
                log.info(mapKey + " doesn't have an autocracy rating, so we'll call it 0.");
            }

            int leadershipSize = 101 - ( aRating * 10 ); // between 1 and 101
//            log.info(leadershipSize + " Leaders in " + mapKey);

            // Create Person nodes with their attributes, tie them to the graph, save them in the db; store them in a deque
            int i = 0;
            try (Transaction tx = db.beginTx()) {
                Node commonWeal = db.createNode(CommonWeal);
                commonWeal.setProperty("name", "Residents of " + mapKey);
                Node computation = db.createNode(Computation);
                computation.setProperty("filename", "Wog.java");
                computation.setProperty("methodname", "createCommonWeal");
                computation.setProperty("name", "WOG");
                computation.createRelationshipTo(commonWeal, COMPUTED);
                commonWeal.createRelationshipTo(territory, REPRESENTS_POPULATION);
                for (Map.Entry<String, Double> e : sortedBC.entrySet()) {
                    String name = e.getKey();
                    Double score = e.getValue();
                    Node pNode = db.createNode(Person);
                    pNode.setProperty("name", name);
                    pNode.setProperty("birthplace", mapKey);
                    pNode.setProperty("bcScore", score);
                    if (i < leadershipSize) {
                        pNode.setProperty("leadershipRole", true);
                    }
                    pNode.createRelationshipTo(commonWeal, RESIDES_IN);
                    Person pax = new Person(pNode);
                    personDeque.add(pax);
                    personMap.put(name, pax);
                    i++;
                }
                tx.success();
            }
//            log.info("Persons have been added to the db and tied to a commonweal, which is tied to this computation and to " + mapKey);

            // Get Tiles for this Territory and sort them by population
            List<Node> tileNodeList = t.getIncludedTileNodes();
            for (Node h : tileNodeList) {
                RelationshipTypes pop = RelationshipTypes.valueOf("SIM_POPULATION_" + y);
                Relationship rp = h.getSingleRelationship(pop, Direction.OUTGOING);
                Node np = rp != null ? rp.getEndNode() : null;
                Integer tilePop = np != null ? ((Number) np.getProperty("value")).intValue() : 0;
                Tile tile = new Tile(h);
                unSortedMap.put(tile, tilePop);
                tileMap.put(tile.getAddress(), tilePop);
            }

            LinkedHashMap<Tile, Integer> sortedTileMap = unSortedMap.entrySet().stream()
                    .filter(e -> e.getValue() > 0)
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, (k, v) -> v, LinkedHashMap::new) );

            log.info(mapKey + " has its " + tileNodeList.size() + " tiles and they are sorted by population");

            // Collect data elements to build a Zipf distributed count of Persons for each tile, ranked by population
            int numTiles = sortedTileMap.size();
            if (numTiles > 500) {
                sortedTileMap = sortedTileMap.entrySet().stream().limit(500)
                        .collect(Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, (k, v) -> v, LinkedHashMap::new) );
            }
            numTiles = sortedTileMap.size();
//            log.info("numTiles: " + numTiles);

            if (numTiles > 1) {
                ZipfDistribution zd = makeZipf(sortedTileMap.size(), 0.90);
                int[] levels = zd.sample(numTiles);
                log.info("Zipf levels: " + levels.length + " for " + mapKey + " and it's " + sortedTileMap.size() + " Tiles.");
                Integer[] iLevels = Arrays.stream(levels).boxed().toArray(Integer[]::new);
                int levelSum = IntStream.of(levels).sum();
//                log.info("levelSum: " + levelSum);
                Arrays.sort(iLevels, Collections.reverseOrder());
                double proportion = size.doubleValue() / levelSum;
//                log.info("proportion: " + proportion);

                int j = 0;
                int checker = 0;
                for (Map.Entry<Tile, Integer> e : sortedTileMap.entrySet()) {
                    if (j < numTiles) {
                        int count = (int) Math.round(iLevels[j] * proportion);
                        repCounts.put(e.getKey(), count);
                        checker += count;
                        j++;
                    }
                }
//            log.info("Checker count = " + checker + " and the counts for each tile are: \n");
//            log.info(repCounts.toString());
                // Because of rounding, there may not have been enough Persons distributed to Tiles; so they are added to the first one.
                Tile first  = sortedTileMap.entrySet().iterator().next().getKey();
                int diff = size.intValue() - checker;
                int firstCount = repCounts.get(first);
                repCounts.put(first, firstCount + diff);
//            log.info("Tiles have been assigned counts and the deque size is " + personDeque.size());
            } else {
                log.info("A single level @ 1000 for " + mapKey + " and it's " + sortedTileMap.size() + " Tiles.");
                if (sortedTileMap.entrySet().iterator().hasNext()) {
                    Tile only = sortedTileMap.entrySet().iterator().next().getKey();
                    repCounts.put(only, 1000);
                }
            }

            int pc = 0;
            int tc = 0;
            // With probability pX, assign most connected Persons to most populated tiles.
//            Instant b4Loop = Instant.now();
//            Deque<Person> rewind = new LinkedList<>();
            for (Tile tile : sortedTileMap.keySet()) {
                if (personDeque.size() > 0) {
                    try (Transaction tx = db.beginTx()) {
                        int count = repCounts.get(tile);
                        String address = tile.getAddress();
//                        log.info(count + " Persons on Tile at " + address);
                        Instant b4Assignment = Instant.now();
                        for (int x=0;x<count;x++) {
                            if (personDeque.size() > 0) {
                                Person pax = personDeque.pop();
//                                if (random.nextGaussian() < 0.80) {
                                    pax.getUnderlyingNode().setProperty("address", address);
                                    personAddressMap.put(pax, address);
//                                    x++;
                                    pc++;
//                                } else {
//                                    rewind.add(pax);
//                                }
                            }
                        }

                        tc++;
//                        Instant aftAssignment = Instant.now();
//                        log.info(address + " took " + Duration.between(b4Assignment, aftAssignment).toMillis() + "; " +
//                                (sortedTileMap.size() - tc) + " tiles left and " + personDeque.size() +
//                                " Persons left to settle.");
                        tx.success();
                    }
                }
            }
            message = mapKey + " has " + pc + " Persons on " + tc + " tiles.";
//            log.info(message + "\n\t and the relationships are about to be added, weighted, then saved");
//            log.info("The tileMap has " + tileMap.size() + ", the personMap has " + personMap.size() + ", and the  ");

            // Iterate over the entire social network to save relationships between these Person nodes.
            try(Transaction tx = db.beginTx()) {
//                Instant b4Graph = Instant.now();
                for(DefaultEdge e : localGraph.edgeSet()) {
                    Relationship ab;
                    double r;
                    Person a = personMap.get(localGraph.getEdgeSource(e) );
                    String addA = personAddressMap.get(a);
                    Person b = personMap.get(localGraph.getEdgeTarget(e) );
                    String addB = personAddressMap.get(b);
                    double bca = a.getBcScore();
                    double bcb = b.getBcScore();
//                    log.info("Linking " + a.toString() + " to " + b.toString() );
                    if (bca > bcb) {
                        r = (tileMap.get(addB) * 1.0) / (tileMap.get(addB) * 1.0);
                        ab = a.getUnderlyingNode().createRelationshipTo(b.getUnderlyingNode(), KNOWS);
                        ab.setProperty("popRatio", r);
                    } else {
                        r = (tileMap.get(addA) * 1.0) / (tileMap.get(addB) * 1.0);
                        ab = b.getUnderlyingNode().createRelationshipTo(a.getUnderlyingNode(), KNOWS);
                        ab.setProperty("popRatio", r);
                    }
                }
//                log.info(" network relationships were saved to the db in " + Duration.between(b4Graph, Instant.now()).toMillis());
                tx.success();
            }
        }
        return Stream.of(new StringResult(message));
    }

    @Procedure(name="wog.writeHydeFacts", mode=Mode.WRITE)
    @Description("CALL wog.writeHydePops(Node dataset, String mapYear, String property, Boolean write) \n" +
            "Acceptable property values are: population, urbanPopulation, wealth, gtp, builtArea")
    public Stream<StringResult> writeHydeFacts(
            @Name("dataset") Node dataset,
            @Name("mapYear") String mapYear,
            @Name("property") String prop,
            @Name("write") Boolean write
    ) throws InterruptedException {
        /**
         *  Given the Dataset node to which these data are attributable, the Year to which the data should be attributed,
         *  the property (specific type of Fact) and a boolean declaration for whether the resulting data should be
         *  written out into a GeoJSON FeatureCollection data file: will read the HYDE data, find the resolution 4 Uber
         *  H3 tile to which the data should be attributed, aggregate HYDE values for that tile, and write a Fact node
         *  into the database and connects the new Fact to the tile, the year, the data source, and the computation
         *  event that created it (e.g., this procedure at date/time).
         */
        String message = "Processing dataset ";
        Dataset source = new Dataset(dataset);
        String file = source.getFilename();
        Boolean wo = write;
        Long year = source.getYear();
        Integer iYear = Integer.parseInt(mapYear);
        String filepath = "/home/cw/Code/george/src/main/resources/hydeData/";
        File f = new File(filepath + file);
        Map<String, Tile> tileMap = new HashMap<>();
        Map<GeoCoord, Double> data = new HashMap<>();
        Map<String, Tile> missingTiles = new HashMap<>();
        Node y = db.findNode(Year,"name", mapYear);
        String relName = getRelName(mapYear, prop);
        RelationshipTypes rel = getRel(relName);
        Labels factType = getLabel(prop);
        String outFile = filepath + year;
        String missing = filepath + year + "_missing_";

        double step = 1.0 / 12.0;
        double offset = 1.0 / 24.0;
        double lon = -180.00 + offset;
        double lat = 90.0 - offset;
        int head = 0;
        int lines = 0;

        // get tiles to which new facts will be linked
        ResourceIterator<Node> tileNodes = db.findNodes(Tile);
        while (tileNodes.hasNext()) {
            Tile t = new Tile(tileNodes.next());
            if (!t.isWater()) {
                String a = t.getAddress();
                tileMap.put(a,t);
            }
        }
        // get the nonempty data from the source and give it a point location
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                data.putAll(parseDataRow(line, lat, lon, step));
                lines++;
                lat -= step;
            }
        } catch (IOException e) {
            message = file + " could not open file to be processed. ";
            return Stream.of(new StringResult(message));
        }
        // attribute data to tiles
        for (Map.Entry<GeoCoord, Double> entry : data.entrySet()) {
            GeoCoord point = entry.getKey();
            String address = getTileAddress(point);
            double val = entry.getValue();
            if (val > 0) {
                Tile t;
                // if the HYDE grid center is in the tile, add the value to the tile's value...
                if (tileMap.containsKey(address)) {
                    t = tileMap.get(address);
                    t.addComputedValue(val);
                    // ...if the HYDE grid center is not in an existing tile, we need to find the closest neighboring tile and
                    // put it there. For example, Palermo is outside of the tiles included in Italy/Sicily, so we need to
                    // add those (two) grid cell's values to H3 tile 841e9a7ffffffff, which is the closest included tile to those
                    // grid cells.
                } else {
                    String current = address;
                    for (int i=1;i<6;i++) {
                        boolean first = true;
                        double min = 99999.99;
                        List<String> neighbors = findNeighbors(address, i);
                        for (String a : neighbors) {
                            if (tileMap.containsKey(a)) {
                                if (first) {
                                    current = a;
                                    min = measureDistance(point, Objects.requireNonNull(getCenterGC(a)));
                                    first = false;
                                } else {
                                    double comp = measureDistance(point, Objects.requireNonNull(getCenterGC(a)));
                                    if (comp < min) {
                                        min = comp;
                                        current = a;
                                    }
                                }
                            } // not this neighbor
                        }
                        if (!address.equals(current)) { // if we've found a close tile to use, break out of the loop
                            i = 6;
                            t = tileMap.get(current);
                            t.addComputedValue(val);
                        }
                    }
                    if (address.equals(current)) {
                        Node n = db.createNode(Tile);
                        n.setProperty("address", address);
                        n.addLabel(MissingTile);
                        t = new Tile(n);
                        t.configureTile();
                        t.addComputedValue(val);
                        missingTiles.put(address, t);
                        tileMap.put(address, t);
                    }
                }
            }
        }

        Node c;
        try (Transaction tx = db.beginTx()) {
            c = db.createNode(Labels.Computation);
            c.setProperty("methodname", "wog.writeHydeFacts");
            c.setProperty("time", LocalDateTime.now());
            dataset.createRelationshipTo(c,CONTRIBUTES_TO);
            dataset.setProperty("outputFile", outFile + prop + "_polygons.geojson");
            tx.success();
        }

        int factotal = 0;
        for (Map.Entry entry : tileMap.entrySet()) {
            try (Transaction tx = db.beginTx()) {
                Tile t = (Tile) entry.getValue();
                String address = (String) entry.getKey();
                Node tn = t.getUnderlyingNode();
                Node fn = db.createNode(Fact);
                factotal++;
                fn.addLabel(factType);
                fn.setProperty("subject", address);
                fn.setProperty("predicate", relName);
                fn.setProperty("object", mapYear);
                fn.setProperty("value", t.getComputedValue(prop));
                fn.setProperty("during", iYear);
                fn.setProperty("name", "Simulated " + prop);
                Relationship rt = tn.createRelationshipTo(fn, rel);
                rt.setProperty("during", iYear);
                Relationship ry = fn.createRelationshipTo(y, DURING);
                ry.setProperty("year", iYear);
                c.createRelationshipTo(fn, COMPUTED);
                tx.success();
            }
        }

        for (Map.Entry entry : missingTiles.entrySet()) {
            try (Transaction tx = db.beginTx()) {
                Tile t = (Tile) entry.getValue();
                Node tn = t.getUnderlyingNode();
                tn.addLabel(MissingTile);
                tx.success();
            }
        }

        if (wo) {
            saveOut(tileMap, outFile, prop);
            saveOut(missingTiles, missing, prop);
        }

        message += "all of the file data, creating " + factotal + " new facts; also found "
                + missingTiles.size() + " missing Tiles.";
        return Stream.of(new StringResult(message));
    }

    @Procedure(name="wog.updateHydeFacts", mode=Mode.WRITE)
    @Description("CALL wog.writeHydePops(Node dataset, String mapYear, String property, Boolean write) \n" +
            "Acceptable property values are: population, urbanPopulation, wealth, gtp, builtArea")
    public Stream<StringResult> updateHydeFacts(
            @Name("dataset") Node dataset,
            @Name("mapYear") String mapYear,
            @Name("property") String prop,
            @Name("write") Boolean write
    ) throws InterruptedException {
        /**
         *  Given the Dataset node to which these data are attributable, the Year to which the data should be attributed,
         *  the property (specific type of Fact) and a boolean declaration for whether the resulting data should be
         *  written out into a GeoJSON FeatureCollection data file: will read the HYDE data, find the resolution 4 Uber
         *  H3 tile to which the data should be attributed, aggregate HYDE values for that tile, and write a Fact node
         *  into the database and connects the new Fact to the tile, the year, the data source, and the computation
         *  event that created it (e.g., this procedure at date/time).
         */
        String message = "Processing dataset ";
        Dataset source = new Dataset(dataset);
        String file = source.getFilename();
        Boolean wo = write;
        Long year = source.getYear();
        Integer iYear = Integer.parseInt(mapYear);
        String filepath = "/home/cw/Code/george/src/main/resources/hydeData/";
        File f = new File(filepath + file);
        Map<String, Tile> tileMap = new HashMap<>();
        Map<GeoCoord, Double> data = new HashMap<>();
        Map<String, Tile> missingTiles = new HashMap<>();
        Node y = db.findNode(Year,"name", mapYear);
        String relName = getRelName(mapYear, prop);
        RelationshipTypes rel = getRel(relName);
        Labels factType = getLabel(prop);
        String outFile = filepath + year;
        String missing = filepath + year + "_missing_";

        double step = 1.0 / 12.0;
        double offset = 1.0 / 24.0;
        double lon = -180.00 + offset;
        double lat = 90.0 - offset;
        int head = 0;
        int lines = 0;

        // get tiles to which new facts will be linked
        ResourceIterator<Node> tileNodes = db.findNodes(Tile);
        while (tileNodes.hasNext()) {
            Tile t = new Tile(tileNodes.next());
            if (!t.isWater()) {
                String a = t.getAddress();
                tileMap.put(a,t);
            }
        }
        // get the nonempty data from the source and give it a point location
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                data.putAll(parseDataRow(line, lat, lon, step));
                lines++;
                lat -= step;
            }
        } catch (IOException e) {
            message = file + " could not open file to be processed. ";
            return Stream.of(new StringResult(message));
        }
        // attribute data to tiles
        for (Map.Entry<GeoCoord, Double> entry : data.entrySet()) {
            GeoCoord point = entry.getKey();
            String address = getTileAddress(point);
            double val = entry.getValue();
            if (val > 0) {
                Tile t;
                // if the HYDE grid center is in the tile, add the value to the tile's value...
                if (tileMap.containsKey(address)) {
                    t = tileMap.get(address);
                    t.addComputedValue(val);
                    // ...if the HYDE grid center is not in an existing tile, we need to find the closest neighboring tile and
                    // put it there. For example, Palermo is outside of the tiles included in Italy/Sicily, so we need to
                    // add those (two) grid cell's values to H3 tile 841e9a7ffffffff, which is the closest included tile to those
                    // grid cells.
                } else {
                    String current = address;
                    for (int i=1;i<6;i++) {
                        boolean first = true;
                        double min = 99999.99;
                        List<String> neighbors = findNeighbors(address, i);
                        for (String a : neighbors) {
                            if (tileMap.containsKey(a)) {
                                if (first) {
                                    current = a;
                                    min = measureDistance(point, Objects.requireNonNull(getCenterGC(a)));
                                    first = false;
                                } else {
                                    double comp = measureDistance(point, Objects.requireNonNull(getCenterGC(a)));
                                    if (comp < min) {
                                        min = comp;
                                        current = a;
                                    }
                                }
                            } // not this neighbor
                        }
                        if (!address.equals(current)) { // if we've found a close tile to use, break out of the loop
                            i = 6;
                            t = tileMap.get(current);
                            t.addComputedValue(val);
                        }
                    }
                    if (address.equals(current)) {
                        Node n = db.createNode(Tile);
                        n.setProperty("address", address);
                        n.addLabel(MissingTile);
                        t = new Tile(n);
                        t.configureTile();
                        t.addComputedValue(val);
                        missingTiles.put(address, t);
                        tileMap.put(address, t);
                    }
                }
            }
        }

        Node c;
        try (Transaction tx = db.beginTx()) {
            c = db.createNode(Labels.Computation);
            c.setProperty("methodname", "wog.writeHydeFacts");
            c.setProperty("time", LocalDateTime.now());
            dataset.createRelationshipTo(c,CONTRIBUTES_TO);
            dataset.setProperty("outputFile", outFile + prop + "_polygons.geojson");
            tx.success();
        }

        int factotal = 0;
        for (Map.Entry entry : tileMap.entrySet()) {
            try (Transaction tx = db.beginTx()) {
                Tile t = (Tile) entry.getValue();
                String address = (String) entry.getKey();
                Node tn = t.getUnderlyingNode();
                Node fn = db.createNode(Fact);
                factotal++;
                fn.addLabel(factType);
                fn.setProperty("subject", address);
                fn.setProperty("predicate", relName);
                fn.setProperty("object", mapYear);
                fn.setProperty("value", t.getComputedValue(prop));
                fn.setProperty("during", iYear);
                fn.setProperty("name", "Simulated " + prop);
                Relationship rt = tn.createRelationshipTo(fn, rel);
                rt.setProperty("during", iYear);
                Relationship ry = fn.createRelationshipTo(y, DURING);
                ry.setProperty("year", iYear);
                c.createRelationshipTo(fn, COMPUTED);
                tx.success();
            }
        }

        for (Map.Entry entry : missingTiles.entrySet()) {
            try (Transaction tx = db.beginTx()) {
                Tile t = (Tile) entry.getValue();
                Node tn = t.getUnderlyingNode();
                tn.addLabel(MissingTile);
                tx.success();
            }
        }

        if (wo) {
            saveOut(tileMap, outFile, prop);
            saveOut(missingTiles, missing, prop);
        }

        message += "all of the file data, creating " + factotal + " new facts; also found "
                + missingTiles.size() + " missing Tiles.";
        return Stream.of(new StringResult(message));
    }

    @Procedure(name="wog.exportTerritoryDataGeoJSON", mode=Mode.READ)
    @Description("CALL wog.exportTerritoryDataGeoJSON(Node territory, String filename)\n" +
            "Acceptable property values are: population, urbanPopulation, wealth, gtp, builtArea")
    public Stream<StringResult> exportTerritoryDataAsGeoJSON(
            @Name("territory") Node territory,
            @Name("filename") String filename
    ) throws InterruptedException {
        Territory t = new Territory(territory);
        int year = t.getYear();
        String fn = filename;
        String filepath = "/home/cw/Code/george/src/main/resources/territory/" + filename;
        List<Node> tileNodes = t.getIncludedTileNodes();
        Map<String, Tile> tileMap = new HashMap<>();

        for (Node n : tileNodes) {
            Tile tile = new Tile(n);
            tileMap.put(tile.getAddress(), tile);
        }

        saveOut(tileMap, filepath, year);

        return Stream.of(new StringResult("saved that into the territory folder"));
    }

    @Procedure(name="wog.getTileDataMap", mode=Mode.READ)
    @Description("CALL wog.getTileDataMap(Node tile, Long year)")
    public Stream<MapResult> getTileDataMap(
            @Name("tile") Node tile,
            @Name("year") Long year
    ) throws InterruptedException {
        Node n = tile;
        String y = year.toString();
        Map<String, Object> data = putTileData(n, y);

        return Stream.of(new MapResult(data));
    }

    @Procedure(name="wog.getTileWithDataFromTerritory", mode=Mode.READ)
    @Description("CALL wog.getTilesWithDataFromTerritory(Node territory)")
    public Stream<MapResult> getTileAndDataMap(
            @Name("territory") Node territory
    ) throws InterruptedException {
        Node territoryNode = territory;
        Territory territoryObject =  new Territory(territoryNode);
        String year = territoryObject.getYear() + "";
        List<Node> nodes = territoryObject.getTileNodeList();
        List<MapResult> results = new ArrayList<>();
        for (Node n : nodes) {
            results.add(new MapResult(putTileData(n, year)));
        }

        return results.stream();
    }

    @Procedure(name="wog.writeTileWealthFact", mode=Mode.WRITE)
    @Description("CALL wog.writeTileWealthFact(Node tile, Node year, Double alpha, Double beta, " +
            "Double ruralLaborRate, Double urbanLaborRate)")
    public Stream<NodeResult> writeTileWealthFact(
            @Name("tile") Node tile,
            @Name("year") Node year,
            @Name("alpha") Double alpha,
            @Name("beta") Double beta,
            @Name("ruralLaborRate") Double ruralLaborRate,
            @Name("urbanLaborRate") Double urbanLaborRate
    ) throws InterruptedException {
        Tile t = new Tile(tile);
        Year y = new Year(year);
        Fact f = new Fact(db.createNode(WealthFact));

        Double a = alpha;
        Double b = beta;
        Double agLabor = ruralLaborRate;
        Double urbLabor = urbanLaborRate;
        int yint = y.getYearAsInt();
        String pred = "SIM_WEALTH_" + y.getName();
        Double pop = t.getPopulationFactValue(yint);
        Double uPop = t.getUrbanPopFactValue(yint);
        Double gtp = t.getGTPFactValue(yint);
        Double L = (agLabor * (pop - uPop)) + urbLabor * uPop;
        Double bLogL = b * Math.log(L);
        Double logY = Math.log(gtp);
        f.setValue(Math.exp( (logY - bLogL) / a) );
        f.setName("Wealth Fact");
        f.setDuring(yint);
        f.setPredicate(pred);
        f.setSubject(t.getAddress());
        f.setObject(y.getName());

        return Stream.of(new NodeResult(f.getUnderlyingNode()));
    }

//    CALL apoc.periodic.iterate(
//            "MATCH (tile:Tile)-[pr]-(pf:PopulationFact) WHERE pf.value > 0.0 RETURN DISTINCT tile",
//            "MATCH (d:Dataset{name:'Estimated Tile Wealth'})
//    WITH tile, d, [1816, 1880, 1914, 1938, 1945, 1994] AS yrs
//    UNWIND yrs AS yr
//    MATCH (tile)-[pr]-(pf:PopulationFact{during:yr})-[dd:DURING{year:yr}]-(y:Year) WHERE pf.value > 0.0 AND pr.during=yr
//    MATCH (tile)-[gr]-(gf:GrossTileProductionFact{during:yr}) WHERE gf.value > 0.0
//    OPTIONAL MATCH (tile)-[ur]-(uf:UrbanPopulationFact{during:yr})
//    WITH d, y, tile, yr, gf.value AS gtp, pf.value AS pop, uf.value AS upop, pf.value-uf.value AS rpop, 'SIM_WEALTH_' + y.name AS pred
//    WITH d, y, tile, yr, pred, gtp, upop, rpop, (rpop*0.65)+(upop*0.75) AS lab, log(gtp) AS logGtp
//    WITH d, y, tile, yr, pred, logGtp, 0.75*log(lab) AS aLogL
//    MERGE (d)-[:CONTRIBUTES{asOf:datetime()}]->(wf:WealthFact:Fact{
//        subject:tile.address,
//                predicate:pred,
//                object:y.name,
//                name:'Estimated Wealth',
//                ruralLaborRate:0.65,
//                urbanLaborRate:0.75,
//                laborElasticity:0.75,
//                capitalElasticity:0.25,
//                value:0.0
//    })-[:DURING{year:yr}]->(y)
//    SET wf.value = exp(logGtp-aLogL)/0.25
//    FOREACH(yx IN CASE WHEN yr=1816 THEN [1] ELSE [] END | MERGE (tile)-[:SIM_WEALTH_1816{during:1816}]->(wf))
//    FOREACH(yx IN CASE WHEN yr=1880 THEN [1] ELSE [] END | MERGE (tile)-[:SIM_WEALTH_1880{during:1880}]->(wf))
//    FOREACH(yx IN CASE WHEN yr=1914 THEN [1] ELSE [] END | MERGE (tile)-[:SIM_WEALTH_1914{during:1914}]->(wf))
//    FOREACH(yx IN CASE WHEN yr=1938 THEN [1] ELSE [] END | MERGE (tile)-[:SIM_WEALTH_1938{during:1938}]->(wf))
//    FOREACH(yx IN CASE WHEN yr=1945 THEN [1] ELSE [] END | MERGE (tile)-[:SIM_WEALTH_1945{during:1945}]->(wf))
//    FOREACH(yx IN CASE WHEN yr=1994 THEN [1] ELSE [] END | MERGE (tile)-[:SIM_WEALTH_1994{during:1994}]->(wf))",
//    {batchSize:100, parallel:true}
//        )

//MATCH (t:Tile)-[:SIM_PRODUCTION_1816]-(gf:GrossTileProductionFact)-[:DURING]-(y:Year), (d:Dataset{name:"Estimated Tile Wealth"})
//    WHERE NOT (t)-[:SIM_WEALTH_1816]-(:WealthFact)
//    OPTIONAL MATCH (t)-[:SIM_POPULATION_1816]-(pf:PopulationFact)
//    OPTIONAL MATCH (t)-[:SIM_URBAN_POPULATION_1816]-(uf:UrbanPopulationFact)
//    WITH t, d, y, log(gf.value) AS lgtp, pf.value-uf.value AS rpop, uf.value AS upop
//    WITH t, d, y, logGTP, log((rpop*0.65)+(upop*0.75))*0.75 AS aLogL
//    MERGE (d)-[:CONTRIBUTES{asOf:datetime()}]->(wf:WealthFact:Fact{
//        subject:t.address,
//                predicate:"SIM_WEALTH_1816",
//                object:y.name,
//                name:"Estimated Wealth",
//                ruralLaborRate:0.65,
//                urbanLaborRate:0.75,
//                laborElasticity:0.75,
//                capitalElasticity:0.25,
//                value:exp(logGTP-aLogL)/0.25
//    })-[:DURING{year:1816}]->(y)
//    MERGE (t)-[:SIM_WEALTH_1816]->(wf)



    // -------------------------------------------- METHODS -------------------------------------------------------- //

    private Map<String, Object> putTileData(Node tile, String year) {
        Node n = tile;
        String y = year;
        Map<String, Object> data = new HashMap<>();

        RelationshipTypes pop = RelationshipTypes.valueOf("SIM_POPULATION_" + y);
        RelationshipTypes uPop = RelationshipTypes.valueOf("SIM_URBAN_POPULATION_" + y);
        RelationshipTypes wealth = RelationshipTypes.valueOf("SIM_WEALTH_" + y);
        RelationshipTypes gtp = RelationshipTypes.valueOf("SIM_PRODUCTION_" + y);
        RelationshipTypes bua = RelationshipTypes.valueOf("SIM_BUILT_AREA_" + y);

        Relationship rp = n.getSingleRelationship(pop, Direction.OUTGOING);
        Relationship ru = n.getSingleRelationship(uPop, Direction.OUTGOING);
        Relationship rw = n.getSingleRelationship(wealth, Direction.OUTGOING);
        Relationship rg = n.getSingleRelationship(gtp, Direction.OUTGOING);
        Relationship rb = n.getSingleRelationship(bua, Direction.OUTGOING);

        Node np = rp != null ? rp.getEndNode() : null;
        Node nu = ru != null ? ru.getEndNode() : null;
        Node nw = rw != null ? rw.getEndNode() : null;
        Node ng = rg != null ? rg.getEndNode() : null;
        Node nb = rb != null ? rb.getEndNode() : null;

        Long a = ((Number) tile.getProperty("h3Id")).longValue();
        Double p = np != null ? ((Number) np.getProperty("value")).doubleValue() : 0.0;
        Double u = nu != null ? ((Number) nu.getProperty("value")).doubleValue() : 0.0;
        Double w = nw != null ? ((Number) nw.getProperty("value")).doubleValue() : 0.0;
        Double g = ng != null ? ((Number) ng.getProperty("value")).doubleValue() : 0.0;
        Double b = nb != null ? ((Number) nb.getProperty("value")).doubleValue() : 0.0;

        data.put("a", a);
        data.put("p", p);
        data.put("u", u);
        data.put("w", w);
        data.put("g", g);
        data.put("b", b);

        return data;
    }


    private List<String> findNeighbors(String a, int i) {
        int out = i;
        String address = a;
        List<String> neighbors = new ArrayList<>();
        try {
            H3Core h3 = H3Core.newInstance();
            neighbors = h3.hexRing(address, out);
            return neighbors;
        } catch (Exception e) {
            log.debug("Couldn't find neighbors for " + address);
        }
        return neighbors;
    }

    private double measureDistance(GeoCoord gc, GeoCoord nc) {
        // Approximate Equirectangular -- works if (lat1,lon1) ~ (lat2,lon2)
        // https://stackoverflow.com/a/4339615/733846
        int R = 6371; // km
        double x = (nc.lng - gc.lng) * Math.cos((gc.lat + nc.lat) / 2);
        double y = (nc.lat - gc.lat);
        double distance = Math.sqrt(x * x + y * y) * R;
        return  distance;
    }

    private String saveOut(Map<String, Tile> map, String fp, String p) {
        FeatureCollection pointFeatures = new FeatureCollection();
        FeatureCollection polyFeatures  = new FeatureCollection();
        String newFile = fp;
        String property = p;
        String json =  "{}";

        for (Map.Entry entry : map.entrySet()) {
            String address = (String) entry.getKey();
            Double val = ((Tile) entry.getValue()).getComputedValue(property);
            Feature pointFeature = new Feature();
            pointFeature.setId(address);
            pointFeature.setProperty(property, val);
            pointFeature.setGeometry(getTileCenter(address));
            pointFeatures.add(pointFeature);
            Feature polyFeature = new Feature();
            polyFeature.setId(address);
            polyFeature.setProperty(property,val);
            polyFeature.setGeometry(getTilePoly(address));
            polyFeatures.add(polyFeature);
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(new File(newFile + property + "_polygons.geojson"), polyFeatures);
            log.info("Wrote out another geojson file of polygons to " + newFile);
        } catch (Exception e) {
            log.debug("I can't write that out to a geojson file, silly.");
            e.printStackTrace();
        }
        return json;
    }

    private String saveOut(Map<String, Tile> map, String fp, int y) {
        FeatureCollection feature = new FeatureCollection();
        ObjectMapper mapper = new ObjectMapper();
        String newFile = fp;
        int year = y;
        String json =  "{}";

        for (Map.Entry entry : map.entrySet()) {
            String address = (String) entry.getKey();
            Tile t = (Tile) entry.getValue();

            Map<String, Object> props = new HashMap<>();
            props.put("address", address);
            props.put("population", t.getPopulationFactValue(year));
            props.put("urbanPop", t.getUrbanPopFactValue(year));
            props.put("wealth", t.getWealthFactValue(year));
            props.put("gtp", t.getGTPFactValue(year));
            props.put("builtArea", t.getBuiltAreaFactValue(year));

            Feature polyFeature = new Feature();
            polyFeature.setId(address);
            polyFeature.setProperties(props);
            polyFeature.setGeometry(getTilePoly(address));
            feature.add(polyFeature);
        }

        try {
            mapper.writeValue(new File(newFile + "_" + year + "_data.geojson"), feature);
            log.info("Wrote out another geojson file of polygons to " + newFile);
        } catch (Exception e) {
            log.debug("I can't write that out to a geojson file, silly.");
            e.printStackTrace();
        }
        return json;
    }

    private String getRelName(String y, String p) {
        String rType = "SIM_";
        switch (p) {
            case "population" : return rType + "POPULATION_" + y;
            case "urbanPopulation" : return rType + "URBAN_POPULATION_"+ y;
            case "wealth" : return rType + "WEALTH_"+ y;
            case "gtp" : return rType + "PRODUCTION_"+ y;
            case "builtArea" : return rType + "BUILT_AREA_"+ y;
        }
        return rType + y;
    }

    private RelationshipTypes getRel(String r) {
        switch (r) {
            case "SIM_POPULATION_1816" : return SIM_POPULATION_1816;
            case "SIM_POPULATION_1850" : return SIM_POPULATION_1850;
            case "SIM_POPULATION_1880" : return SIM_POPULATION_1880;
            case "SIM_POPULATION_1914" : return SIM_POPULATION_1914;
            case "SIM_POPULATION_1938" : return SIM_POPULATION_1938;
            case "SIM_POPULATION_1945" : return SIM_POPULATION_1945;
            case "SIM_POPULATION_1994" : return SIM_POPULATION_1994;
            case "SIM_URBAN_POPULATION_1816" : return SIM_URBAN_POPULATION_1816;
            case "SIM_URBAN_POPULATION_1850" : return SIM_URBAN_POPULATION_1850;
            case "SIM_URBAN_POPULATION_1880" : return SIM_URBAN_POPULATION_1880;
            case "SIM_URBAN_POPULATION_1914" : return SIM_URBAN_POPULATION_1914;
            case "SIM_URBAN_POPULATION_1938" : return SIM_URBAN_POPULATION_1938;
            case "SIM_URBAN_POPULATION_1945" : return SIM_URBAN_POPULATION_1945;
            case "SIM_URBAN_POPULATION_1994" : return SIM_URBAN_POPULATION_1994;
            case "SIM_WEALTH_1816" : return SIM_WEALTH_1816;
            case "SIM_WEALTH_1850" : return SIM_WEALTH_1850;
            case "SIM_WEALTH_1880" : return SIM_WEALTH_1880;
            case "SIM_WEALTH_1914" : return SIM_WEALTH_1914;
            case "SIM_WEALTH_1938" : return SIM_WEALTH_1938;
            case "SIM_WEALTH_1945" : return SIM_WEALTH_1945;
            case "SIM_WEALTH_1994" : return SIM_WEALTH_1994;
            case "SIM_PRODUCTION_1816" : return SIM_PRODUCTION_1816;
            case "SIM_PRODUCTION_1850" : return SIM_PRODUCTION_1850;
            case "SIM_PRODUCTION_1880" : return SIM_PRODUCTION_1880;
            case "SIM_PRODUCTION_1914" : return SIM_PRODUCTION_1914;
            case "SIM_PRODUCTION_1938" : return SIM_PRODUCTION_1938;
            case "SIM_PRODUCTION_1945" : return SIM_PRODUCTION_1945;
            case "SIM_PRODUCTION_1994" : return SIM_PRODUCTION_1994;
            case "SIM_BUILT_AREA_1816" : return SIM_BUILT_AREA_1816;
            case "SIM_BUILT_AREA_1850" : return SIM_BUILT_AREA_1850;
            case "SIM_BUILT_AREA_1880" : return SIM_BUILT_AREA_1880;
            case "SIM_BUILT_AREA_1914" : return SIM_BUILT_AREA_1914;
            case "SIM_BUILT_AREA_1938" : return SIM_BUILT_AREA_1938;
            case "SIM_BUILT_AREA_1945" : return SIM_BUILT_AREA_1945;
            case "SIM_BUILT_AREA_1994" : return SIM_BUILT_AREA_1994;
        }
        return null;
    }

    private Labels getLabel(String r) {
        switch (r) {
            case "population" : return PopulationFact;
            case "urbanPopulation" : return UrbanPopulationFact;
            case "wealth" : return WealthFact;
            case "gtp" : return GrossTileProductionFact;
            case "builtArea" : return BuiltAreaFact;
        }
        return null;
    }

    private String getTileAddress(GeoCoord gc) {
        String address;
        try {
            H3Core h3 = H3Core.newInstance();
            address = h3.geoToH3Address(gc.lat, gc.lng, 4);
        } catch (Exception e) {
            log.debug("GeoCoord " + gc.toString() + " caused a problem.");
            e.printStackTrace();
            return "Bad";
        }
        return address;
    }

    private GeoCoord getCenterGC(String a) {
        GeoCoord gc;
        try {
            H3Core h3 = H3Core.newInstance();
            gc = h3.h3ToGeo(a);
        } catch (Exception e) {
            log.debug(a + " to GeoCoord caused a problem.");
            e.printStackTrace();
            return null;
        }
        return gc;
    }

    private Point getTileCenter(String address) {
        Point point = new Point();
        LngLatAlt lla = new LngLatAlt();
        try {
            H3Core h3 = H3Core.newInstance();
            GeoCoord gc = h3.h3ToGeo(address);
            lla.setLongitude(gc.lng);
            lla.setLatitude(gc.lat);
            point.setCoordinates(lla);
            return point;
        } catch (Exception e) {
            log.debug("Tile " + address + " caused a problem.");
            e.printStackTrace();
            return null;
        }
    }

    private Polygon getTilePoly(String address) {
        // These Resolution 4 address cross the international date line an the resulting poly wraps around the globe.
        List<String> idls = new ArrayList<>( Arrays.asList("840d9edffffffff", "840d917ffffffff", "840d911ffffffff",
                "840d919ffffffff", "840d953ffffffff", "840d95bffffffff", "840d865ffffffff", "840d86dffffffff",
                "840db17ffffffff", "840db11ffffffff", "840d9e5ffffffff", "8417691ffffffff", "8417697ffffffff",
                "840cb6dffffffff", "840db1dffffffff", "840db15ffffffff", "840db3bffffffff", "8404cc3ffffffff",
                "840d86dffffffff", "840db17ffffffff") );
        try {
            H3Core h3 = H3Core.newInstance();
            Polygon poly = new Polygon();
            List<GeoCoord> outerPolyCoords = h3.h3ToGeoBoundary(address);
            List<LngLatAlt> lngLatAlts = new ArrayList<>();
            for (GeoCoord gc : outerPolyCoords) {
                if (idls.contains(address)) {
                    double wrong = gc.lng;
                    if (-179.99791939301488 == wrong) {
                        log.info("As it turns out, " + -179.99791939301488 + " IS less than zero but, " + wrong + " is, too." );
                    } else {
                        log.info("As it turns out, " + wrong + " IS NOT less than zero. Hmmmm.");
                    }
                    if (wrong < 0.0 || wrong == -179.99791939301488 ) {
                        gc = new GeoCoord(gc.lat, 360.0 + wrong);
                    }
                }
                lngLatAlts.add(lngLatAlt(gc));
            }
            lngLatAlts.add(lngLatAlts.get(0));
            poly.setExteriorRing(lngLatAlts);
            return poly;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    static private LngLatAlt lngLatAlt(GeoCoord coordinates) {
        LngLatAlt lla = new LngLatAlt();
        lla.setLatitude(coordinates.lat);
        lla.setLongitude(coordinates.lng);
        return lla;
    }

    private Map<GeoCoord, Double> parseDataRow(String s, double l, double g, double i) {
        /**
         *  Returns a map of GeoCoords and the data at that location. GeoCoords are
         *  @param s the data line to be parsed
         *  @param l the latitude of the data line
         *  @param g the longitude (always starts at -180.0 - (1.0 / 24.0) = 179.958333
         *  @param i the size of the step between HYDE grid centers (1.0 / 12.0 = 0.833)
         *  @return Map<GeoCoord, Double> pointData
         */
        Map<GeoCoord, Double> pointData = new HashMap<>();
        double lat = l;
        double step = i;
        double lon = g;
        String row = s;
        String[] values = row.split(",");
        for (String datum : values) {
            if (!datum.equals("-9999")) {
                GeoCoord gc = new GeoCoord(lat, lon);
                Double val = Double.parseDouble(datum);
                if (val > 0.0000) pointData.put(gc, val);
            }
            lon += step;
        }
        return pointData;
    }

    private Number parseMeta(String s) {
        String[] items = s.split("\t");
        String item = items[0];
        String val = items[1];
        log.debug("The meta parser found " + val);
        if (item.equals("cellsize")) {
            return Double.parseDouble(val);
        } else {
            return Integer.parseInt(val);
        }
    }

    private Node makeTerritoryNode(Territory t) {
        Node territory = null;
        try (Transaction tx = db.beginTx()) {
            territory = db.createNode(Territory);
            territory.setProperty("name", t.getName());
            territory.setProperty("mapKey", t.getMapkey());
            territory.setProperty("year", t.getYear());
            if (t.getAbbr() != null) {
                territory.setProperty("abbr", t.getAbbr());
            }
            if (t.getCowcode() != null) {
                territory.setProperty("cowcode", t.getCowcode());
            }
            if (t.getArea() > 0.0) {
                territory.setProperty("area", t.getArea());
            }
        } catch (Exception e) {
//            log.debug("makeTerritoryNode() is the culprit.");
            e.printStackTrace();
            return null;
        }
        return territory;
    }

    private List<Node> createTileNodes(Territory t) {

        List<Node> tiles = new ArrayList<>();

        try {
            H3Core h3 = H3Core.newInstance();

            for (String a : t.getTiles()) {
                Node tile = null;
                if (db.findNode(Tile, "h3Id", a) == null) {
                    tile = db.createNode(Tile);
                    tile.setProperty("h3Id", a);
                    tile.setProperty("address", h3.stringToH3(a));
                    tile.setProperty("face", h3.h3GetBaseCell(a));
                    tile.setProperty("resolution", t.getResolution());
                } else {
                    tile = db.findNode(Tile, "h3Id", a);
                }
                tiles.add(tile);
            }
        } catch (Exception e) {
//            log.debug("createTileNodes() is the culprit.");
            e.printStackTrace();
        }

        return tiles;
    }

    private Map<String, Territory> purgeTinyTerritories(Map<String, Territory> old) {
        Map<String, Territory> territoryMap = old;
        // Get rid of any territories too small to have any tiles
        List<String> irrelevants = new ArrayList<>();
        for (Territory t : territoryMap.values()) {
            if (t.getTiles().size() <= 0) {
                irrelevants.add(t.getMapkey());
            }
        }
        for (String t : irrelevants) {
            territoryMap.remove(t);
        }

        return territoryMap;
    }

    private static List<org.geojson.Feature> geoJsonProcessor(String filename) {
        // Parse the GeoJSON file
        String filepath = "/home/cw/Code/george/src/main/resources/historicalBasemaps/" + filename;
        File file = new File(filepath);

        List<org.geojson.Feature> features = new ArrayList<>();
        try (InputStream inputStream = new FileInputStream(file)) {
            features = new ObjectMapper().readValue(inputStream, FeatureCollection.class).getFeatures();
        } catch (Exception e) {
            System.out.println("Well, that didn't work. Double-check the filename and the hardcoded path in H3Neo4j.geoJsonProcessor(): " + filepath);
            e.printStackTrace();
        }
        return features;
    }

    private boolean saveTileFactsGraph(Tile tO, Node cN, Node yN) {
        String message = "";
        Tile tile = tO;
        String address = tile.getAddress();
        Node t = tO.getUnderlyingNode();
        Node c = cN;
        Node y = yN;
        String syear = (String) y.getProperty("name");
        int year = Integer.parseInt(syear);
        RelationshipTypes popRel = SIM_POPULATION_1816;
        RelationshipTypes uPopRel = SIM_URBAN_POPULATION_1816;
        RelationshipTypes wealRel = SIM_WEALTH_1816;

        switch (year) {
            case 1880:
                popRel = SIM_POPULATION_1880;
                uPopRel = SIM_URBAN_POPULATION_1880;
                wealRel = SIM_WEALTH_1880;
                break;
            case 1914:
                popRel = SIM_POPULATION_1914;
                uPopRel = SIM_URBAN_POPULATION_1914;
                wealRel = SIM_WEALTH_1914;
                break;
            case 1938:
                popRel = SIM_POPULATION_1938;
                uPopRel = SIM_URBAN_POPULATION_1938;
                wealRel = SIM_WEALTH_1938;
                break;
            case 1945:
                popRel = SIM_POPULATION_1945;
                uPopRel = SIM_URBAN_POPULATION_1945;
                wealRel = SIM_WEALTH_1945;
                break;
            case 1994:
                popRel = SIM_POPULATION_1994;
                uPopRel = SIM_URBAN_POPULATION_1994;
                wealRel = SIM_WEALTH_1994;
                break;
        }

        // Save the territory graph for all tiles, eg: (c:Computation)-[:COMPUTED]-\
        //                            (t:Territory)-[:INCLUDES]->(i:Tile)-[:SIM_*]->(f:Fact)-[:DURING]->(y:Year)

        // Create the population fact
        Node pf = db.createNode(Fact);
        pf.addLabel(PopulationFact);
        pf.setProperty("value", tile.getPopulation());
        pf.setProperty("subject", address);
        pf.setProperty("predicate", ("SIM_POPULATION_" + syear) );
        pf.setProperty("object", "" + year);
        pf.setProperty("name", "Simulated Population");
        pf.setProperty("during", year);
        // Relate it to its tile, the computation, and the year
        Relationship pr = t.createRelationshipTo(pf, popRel);
        pr.setProperty("during", year);
        Relationship pyr = pf.createRelationshipTo(y, DURING);
        pyr.setProperty("during", year);
        Relationship pcr = c.createRelationshipTo(pf, COMPUTED);
        // If there is a value (other than zero) for urban pop, repeat the two steps above for the urban pop fact
        if (tile.getUrbanPopulation() > 0) {
            Node uf = db.createNode(Fact);
            uf.addLabel(UrbanPopulationFact);
            uf.setProperty("value", tile.getUrbanPopulation());
            uf.setProperty("subject", address);
            uf.setProperty("predicate", ("SIM_URBAN_POPULATION" + syear) );
            uf.setProperty("object", "" + year);
            uf.setProperty("name", "Simulated Urban Population");
            uf.setProperty("during", year);

            Relationship ur = t.createRelationshipTo(uf, uPopRel);
            ur.setProperty("during", year);
            Relationship uyr = uf.createRelationshipTo(y, DURING);
            uyr.setProperty("during", year);
            Relationship ucr = cN.createRelationshipTo(uf, COMPUTED);
        }
        // Repeat again if the tile's wealth is greater than zero
        if (tile.getWealth() > 0) {
            Node wf = db.createNode(Fact);
            wf.addLabel(WealthFact);
            wf.setProperty("value", tile.getWealth());
            wf.setProperty("subject", address);
            wf.setProperty("predicate", ("SIM_WEALTH" + syear) );
            wf.setProperty("object", "" + year);
            wf.setProperty("name", "Simulated Wealth");
            wf.setProperty("during", year);

            Relationship wr = t.createRelationshipTo(wf, wealRel);
            wr.setProperty("during", year);
            Relationship wyr = wf.createRelationshipTo(y, DURING);
            wyr.setProperty("during", year);
            Relationship cr = cN.createRelationshipTo(wf, COMPUTED);
        }

        return true;
    }

    private String convertWithIteration(Map<String, Tile> map) {
        int i = 0;
        StringBuilder mapAsString = new StringBuilder("\n{");
        for (String key : map.keySet()) {
            if (i < 3) {
                Tile t = map.get(key);
                mapAsString.append(key + " = " );
                mapAsString.append("pop:" + t.getPopulation() + ", ");
                mapAsString.append("uPop:" + t.getUrbanPopulation() + ", ");
                mapAsString.append("wealth:" + t.getWealth());
                if (i < 2) {
                    mapAsString.append("; \n");
                }
            }
            i++;
        }
        mapAsString.append("}\n");

        return mapAsString.toString();
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, int n) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Collections.reverse(list);
        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list.subList(0, n)) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private ZipfDistribution makeZipf(int s, double e) {
        ZipfDistribution zd;
        try {
            zd = new ZipfDistribution(new MTFApache(new MersenneTwisterFast()), s, e);
        } catch (NotStrictlyPositiveException n) {
            System.out.println("Not enough tiles to make a distribution.");
            return null;
        }
        return zd;
    }


}



