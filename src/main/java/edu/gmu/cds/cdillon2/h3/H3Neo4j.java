package edu.gmu.cds.cdillon2.h3;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.gmu.cds.cdillon2.schema.*;
import edu.gmu.cds.cdillon2.util.MTFApache;
import org.apache.commons.math3.distribution.ZipfDistribution;
import com.uber.h3core.H3Core;
import ec.util.MersenneTwisterFast;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.neo4j.cypher.internal.compiler.v2_3.No;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.time.*;

import edu.gmu.cds.cdillon2.results.*;
import static edu.gmu.cds.cdillon2.schema.Labels.*;
import static edu.gmu.cds.cdillon2.schema.RelationshipTypes.*;



import javax.xml.crypto.Data;

/**
 *
 *
 */
public class H3Neo4j {

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;


    @Procedure(name = "h3neo4j.returnHexAddress", mode = Mode.WRITE)
    @Description("Call h3neo4j.returnHexAddress(latitude, longitude, resolution")
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


    @Procedure(name = "h3neo4j.returnTileNode", mode = Mode.WRITE)
    @Description("Call h3neo4j.returnHexTile(latitude, longitude, resolution")
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


    @Procedure(name = "h3neo4j.polyfillFeaturesFile", mode = Mode.WRITE)
    @Description("Call h3neo4j.polyfillFeaturesFile(filename, year, resolution)")
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

//
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


    @Procedure(name = "h3neo4j.loadBaselinePopulation", mode = Mode.WRITE)
    @Description("Call h3neo4j.loadBaselinePopulation(Node territoryNode, String startYear, String endYear, computationNode")
    public Stream<StringResult> loadBaselinePopulation(
            @Name("territory") Node territory,
            @Name("startYear") String startYear,
            @Name("endYear") String endYear,
            @Name("computation") Node computation
    ) throws InterruptedException {
        String cowcode = (String) territory.getProperty("cowcode");
        String mapKey = (String) territory.getProperty("mapKey");
        int startInt = Integer.parseInt(startYear);
        int endInt = Integer.parseInt(endYear);
        endInt = endInt > startInt ? endInt : endInt + 1;

        String message = "No message";

        if (cowcode != "NA") {
            MersenneTwisterFast random = new MersenneTwisterFast();

            Map<String, Object> params = new HashMap<>();
            params.put("cowcode", cowcode);
            params.put("startYear", startInt);
            params.put("untilYear", endInt);
            params.put("mapKey", mapKey);

            String query = "MATCH (r:Territory{mapKey:$mapKey})-[:INCLUDES]-(t:Tile) RETURN DISTINCT(t)";

            String delPopQuery = "MATCH (r:Territory{mapKey:$mapKey})-[:INCLUDES]-(t:Tile)-[s:SIM_POPULATION]-(pf:PopulationFact)-[d:DURING]-(y:Year) " +
                    "WHERE s.year=$startYear AND d.year=$startYear DELETE d, s, pf";
            String delUpopQuery = "MATCH (r:Territory{mapKey:$mapKey})-[:INCLUDES]-(t:Tile)-[u:SIM_URBAN_POPULATION]-(uf:PopulationFact)-[d:DURING]-(y:Year) " +
                    "WHERE u.year=$startYear AND d.year=$startYear DELETE d, u, uf";

            String popQuery = "MATCH (t:Territory)-[o]-(s:State{cowcode:$cowcode})-[:POPULATION]-(pf:PopulationFact)-[:DURING]-(y:Year)," +
                    "(d:Dataset{name:'NMC Supplemental'})" +
                    "WHERE (d)-[:CONTRIBUTES]-(pf) AND $startYear <= y.began.year < $untilYear " +
                    "WITH pf, y ORDER BY y.began.year " +
                    "RETURN pf.value LIMIT 1";

            // Delete old facts and their relations to tiles and years
            db.execute(delPopQuery, params);
            db.execute(delUpopQuery, params);

            // Setup for new data
            List<Tile> tiles = db.execute(query, params).columnAs("t").stream().map(t -> new Tile((Node) t)).collect(Collectors.toList());
            List<Tile> urbanTiles = new ArrayList<>();

            // Get Territory level population data
            Result popResult = db.execute(popQuery, params);
            Long pop = 0L;
            while (popResult.hasNext()) {
                Object r = popResult.columnAs("pf.value").next();
                pop = (r instanceof Long ) ? (Long) r * 1000 : 0L;
            }
            log.info(mapKey + " has population of " + pop.toString());

            // Calculate some parameters. The total number of tiles is 81,428
            int num = tiles.size();
            int unum = 1;
            double tratio = num / (81428 * 10.0);
            double uxp = 1.07;
            double pxp;
            double popDensity = pop / (num * 1.0);

            if (popDensity < 100000) {
                pxp = uxp;
            } else if (popDensity < 50000) {
                pxp = uxp + tratio;
            } else if (popDensity < 20000) {
                pxp = uxp + (tratio * 2);
            } else if (popDensity < 10000) {
                pxp = uxp + (tratio * 3);
            } else if (popDensity < 5000.0) {
                pxp = uxp + (tratio * 4);
            } else {
                pxp = uxp;
            }

            log.info(mapKey + " has a pop exponent of " + pxp + " and an urban exponent of " + uxp);
            ZipfDistribution distribution = new ZipfDistribution(new MTFApache(random), num, pxp);

            String uPopQuery = "MATCH (t:Territory)-[o]-(s:State{cowcode:$cowcode})-[:URBAN_POPULATION]-(uf:UrbanPopulationFact)-[:DURING]-(y:Year)," +
                    "(d:Dataset{name:'NMC Supplemental'})" +
                    "WHERE (d)-[:CONTRIBUTES]-(uf) AND $startYear <= y.began.year < $untilYear " +
                    "WITH uf, y ORDER BY y.began.year " +
                    "RETURN uf.value LIMIT 1";

            Result uPopResult = db.execute(uPopQuery, params);
            Long uPop = 0L;
            while (uPopResult.hasNext()) {
                Object r = uPopResult.columnAs("uf.value").next();
                log.info(r.toString());
                uPop = (r instanceof Long) ? (Long) r * 1000 : 0L;
            }
            log.info(mapKey + " has an urban population of "+ uPop.toString());

            Tile mostPopulous = null;
            int summedPopulation = 0;
            int summedUpop = 0;

            if (num > 0 && pop > 0) {
                int pacer = 0;
                summedPopulation = 0;
                int[] blevels = distribution.sample(num);
                int[] levels = new int[blevels.length];
                for (int i=0; i<blevels.length; i++) {
                    levels[i] = blevels[i] - 1;
                }
                int distSum = Math.max(IntStream.of(levels).sum(), 1);
                int pProportion = pop.intValue() / distSum;

                for (Tile h : tiles) {
                    // Applying the population proportion to this tile is simple: multiply the level from the samples
                    // array to the proportion of the population represented by each portion of the total
                    int thisPop = pProportion * levels[pacer];
                    summedPopulation += thisPop;
                    h.setPopulation(thisPop);
                    h.setYear(startInt);
                    if (mostPopulous!=null) {
                        if (h.getPopulation() > mostPopulous.getPopulation()) {
                            mostPopulous = h;
                        }
                    } else {
                        mostPopulous = h;
                    }

                    if (thisPop >= 50000 && uPop > 0) {
                        urbanTiles.add(h);
                    }
                    pacer++;
                }
                log.info(mapKey + " has recorded population of " + pop + " and the distributed population total of "
                        + summedPopulation);
            }

            // Figure out the length of the urbanizable tile list
            if (uPop > 0) {
                unum = Math.min( (urbanTiles.size()), ( (int) (uPop / 50000) ) );
                log.info(mapKey + " has " + num + " tiles and " + unum + " urban tiles.");
                if (unum > 1) {
                    urbanTiles.sort(Comparator.comparing(edu.gmu.cds.cdillon2.schema.Tile::getPopulation).reversed());
                    List<Tile> uTiles = new ArrayList<>(urbanTiles.subList(0, unum));
                    ZipfDistribution urbanDistrib = new ZipfDistribution(new MTFApache(random), unum, uxp);

                    int uPacer = 0;
                    summedUpop = 0;
                    int[] uLevels = urbanDistrib.sample(unum);
                    int urbSum = IntStream.of(uLevels).sum();
                    int uProportion = uPop.intValue() / urbSum;

                    for (Tile h : uTiles) {
                        int thisUpop = uProportion * uLevels[uPacer];
                        summedUpop += thisUpop;
                        h.setUrbanPopulation(thisUpop);
                        if (thisUpop > h.getPopulation()) {
                            h.setPopulation(thisUpop);
                        }
                        uPacer++;
                    }
                    log.info(mapKey + " has recorded urban population of " + uPop + " and the distributed urban pop total of " + summedUpop);
                }
                else {
                    // just pick the most populous tile
                    int thisUpop = uPop.intValue();
                    int summedUrbanPop = thisUpop;
                    mostPopulous.setUrbanPopulation(thisUpop);
                    if (thisUpop > mostPopulous.getPopulation()) {
                        mostPopulous.setPopulation(thisUpop);
                    }
                }
            }

            // long utc = Clock.systemUTC().millis();
            String utc = Clock.systemUTC().toString();

            try (Transaction tx = db.beginTx()) {
                Node y = db.findNode(Year, "name", startYear);
                for (Tile h : tiles) {
                    Node pf = h.getPopulationFact(startInt)!=null ? h.getPopulationFact(startInt) : db.createNode(Fact);
                    pf.setProperty("subject", h.getAddress());
                    pf.setProperty("predicate", "SIM_POPULATION");
                    pf.setProperty("object", startYear);
                    pf.setProperty("name", "Simulated Population");
                    pf.setProperty("value", h.getPopulation());
                    pf.setProperty("year", startInt);
                    pf.addLabel(PopulationFact);

                    Relationship rf = h.getPopFactRelation(startInt);
                    if (rf==null) {
                        rf = h.getUnderlyingNode().createRelationshipTo(pf, SIM_POPULATION);
                        rf.setProperty("year", startInt);
                    }
                    Relationship ry = h.getFactDuringRel(y);
                    if (ry==null) {
                        ry = pf.createRelationshipTo(y, DURING);
                        ry.setProperty("year", startInt);
                    }
                    Relationship rt = pf.createRelationshipTo(computation, COMPUTED);
                    rt.setProperty("time", utc);

                    if (h.getUrbanPopulation() > 0) {
                        Node uf = h.getUrbanPopFact(startInt)!=null ? h.getUrbanPopFact(startInt) : db.createNode(Fact);
                        uf.setProperty("predicate", "SIM_URBAN_POPULATION");
                        uf.setProperty("object", startYear);
                        uf.setProperty("name", "Simulated Urban Population");
                        uf.setProperty("value", h.getUrbanPopulation());
                        uf.setProperty("year", startInt);
                        uf.addLabel(UrbanPopulationFact);
                        Relationship ruf = h.getUrbanPopFactRelation(startInt);
                        if (ruf==null) {
                            ruf = h.getUnderlyingNode().createRelationshipTo(uf, SIM_URBAN_POPULATION);
                            ruf.setProperty("year", startInt);
                        }
                        Relationship ruy = h.getFactDuringRel(y);
                        if (ruy==null) {
                            ruy = uf.createRelationshipTo(y, DURING);
                            ruy.setProperty("year", startInt);
                        }
                        Relationship rut = uf.createRelationshipTo(computation, COMPUTED);
                        rut.setProperty("time", utc);
                    }
                }
                tx.success();
            }
            message = mapKey + " has been successfully built with " + summedPopulation + " population and " + summedUpop + " urban population" ;
        } else {
            message = "This territory has no COW";
        }
        return Stream.of(new StringResult(message));
    }

    @Procedure(name = "h3neo4j.polyfillStoredData", mode = Mode.WRITE)
    @Description("Call h3neo4j.polyfillStoredData(String H3 resolution")
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

    @Procedure(name = "h3neo4j.polyfillFeatureCollection", mode = Mode.WRITE)
    @Description("Call h3neo4j.polyfillFeatureCollection(String filename, String year, String H3 resolution)")
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
                    r.setProperty("year", year);
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

    @Procedure(name = "h3neo4j.findDataset", mode = Mode.READ)
    @Description("Call h3neo4j.findDataset(String year)")
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

    @Procedure(name = "h3neo4j.setPopularWealth", mode = Mode.WRITE)
    @Description("Call h3neo4j.setPopularWealth(Node territory, Node computation)")
    public Stream<StringResult> tileWealthByPopulation(
            @Name("territoryNode") Node territoryNode,
            @Name("computation") Node computation
            ) throws InterruptedException {
        String returnMessage = "";
        /**
         * Given a territory, identify it's INCLUDED tiles, estimate the amount of wealth tiles need to have (on average)
         * to pay their taxes for 10 years, create fact and paths:
         * (t:Tile)-[:SIM_WEALTH]->(w:WealthFact:Fact{value: })-[:DURING]->(y:Year),
         * (c:Computation)-[:COMPUTED]->(f)
         */
        Territory t = new Territory(territoryNode);
        returnMessage = t.getMapkey();
        String utc = Clock.systemUTC().toString();
        log.info(returnMessage + " " + " SNAFU at " + utc);
        int y = t.getYear();

        Polity p = new Polity(territoryNode.getSingleRelationship(OCCUPIED, Direction.INCOMING).getOtherNode(territoryNode));
        returnMessage += (" with " + p.getName());
        log.info(returnMessage + " SNAFU at " + utc);

        int milex = p.getMilExFact(y) * 10;
        int totalPop = p.getPopFact(y);
        List<Node> tiles = t.getIncludedTileNodes();
        double territoryWealth = 0.0;

        try (Transaction tx = db.beginTx()) {
            Node yNode = db.findNode(Year, "name", ("" + y) );
            for (Node n : tiles) {
                Tile i = new Tile(n);
                Node f = i.getPopulationFact(y);
                double pop = (Integer) f.getProperty("value") * 1.0;
                double wealth =  (pop / totalPop) * milex;
                Node wf = db.createNode(Fact);
                wf.addLabel(WealthFact);
                wf.setProperty("value", wealth);
                wf.setProperty("during", y);
                wf.setProperty("name", "Simulated Wealth");
                wf.setProperty("subject", (String) n.getProperty("address"));
                wf.setProperty("predicate", "SIM_WEALTH");
                wf.setProperty("object", ("" + y) );

                Relationship wr = n.createRelationshipTo(wf, SIM_WEALTH);
                wr.setProperty("during", y);

                Relationship dr = wf.createRelationshipTo(yNode, DURING);
                dr.setProperty("during", y);

                Relationship cr = computation.createRelationshipTo(wf, COMPUTED);
                cr.setProperty("time", utc);
                territoryWealth += wealth;
            }

            tx.success();
        }

        returnMessage = returnMessage + " now has total simulated wealth is " + territoryWealth;

        return Stream.of(new StringResult(returnMessage));
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
            log.debug("makeTerritoryNode() is the culprit.");
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
            log.debug("createTileNodes() is the culprit.");
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

}

