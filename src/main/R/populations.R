library("RNeo4j")
library("naniar")
library("ggplot2")
library("imputeTS")
library("dplyr")
library("tidyr")
library("data.table")
library("plotly")
library("RColorBrewer")
library("VGAM")

# withr::with_makevars(c(PKG_LIBS = "-liconv"), install.packages("RNeo4j"), assignment = "+=")

## Configure a connection to the graph database
graph <- startGraph("http://localhost:7474/db/data",
                    username="neo4j",
                    password="george")

query <- "MATCH (m:MembershipFact)-[:MEMBER]-(s:State)-[o]-(t:Territory{year:1816})-[:INCLUDES]->(h:Tile)-[:SIM_POPULATION]-(pf:PopulationFact)-[:DURING{year:1816}]-(y:Year{name:'1816'})
WHERE t.cowcode = s.cowcode AND (m.from.year <= 1816 OR m.from.year IS NULL)
WITH s, t, h, pf, y
MATCH (s)-[:POPULATION]-(spf:PopulationFact)-[:DURING]-(y)
OPTIONAL MATCH (h)-[:SIM_URBAN_POPULATION]-(uf:UrbanPopulationFact)-[:DURING]-(y)
OPTIONAL MATCH (s)-[:URBAN_POPULATION]-(suf:UrbanPopulationFact)-[:DURING]-(y)
RETURN t.name AS territories, h.address AS tileId, pf.value AS pop, uf.value AS uPop, spf.value AS tPop, suf.value AS tuPop
UNION
MATCH (m:MembershipFact)-[:MEMBER]-(s:State)-[o]-(t:Territory{year:1816})-[:INCLUDES]->(h:Tile)-[:SIM_POPULATION]-(pf:PopulationFact)-[:DURING{year:1816}]-(y:Year{name:'1816'})-[:DURING]-(uf:UrbanPopulationFact)-[:SIM_URBAN_POPULATION]-(h)
WHERE t.cowcode = s.cowcode AND (m.from.year <= 1816 OR m.from.year IS NULL)
WITH s, t, h, pf, y, uf
MATCH (s)-[:URBAN_POPULATION]-(suf:UrbanPopulationFact)-[:DURING]-(y)-[:DURING]-(spf:PopulationFact)-[:POPULATION]-(s)
RETURN t.name AS territories, h.address AS tileId, pf.value AS pop, uf.value AS uPop, spf.value AS tPop, suf.value AS tuPop"

terr.df <- cypher(graph, query)
terr.df$name <- as.factor(terr.df$territories)
terr.df$popFac <- as.factor(terr.df$pop)
terr.df$uPopFac <- as.factor(terr.df$uPop)
terr.df$ratio <- terr.df$uPop / terr.df$pop
terr.df$ratfac <- as.factor(terr.df$ratio)
terr.df$tuPopFac <- as.factor(terr.df$tuPop)
terr.df$tPopFac <- as.factor(terr.df$tPop)

summary(terr.df)
plot((terr.df$pop / 1000), (terr.df$ratio) )
terr.df[which(terr.df$ratio > 1.0), names(terr.df) %in% c("territories","pop","uPop", "ratio"),]
terr.df[which(terr.df$uPop > 1), names(terr.df) %in% c("territories","pop","uPop", "ratio"),]


uPops.df <- terr.df[which(terr.df$uPop > 1), names(terr.df) %in% c("name","pop","uPop", "tPopFac", "tuPopFac"),]
uPopSums <- uPops.df %>%
  group_by(name, tPopFac, tuPopFac) %>%
  summarise_all(funs(sum))
uPopCounts <- uPops.df %>%
  count(name)
rename(uPopCounts, urbanTiles = n)
uPops <- full_join(uPopSums, uPopCounts, by="name", copy=FALSE, suffix=c(".sums", ".counts"))

pops.df <- terr.df[which(terr.df$pop > 1), names(terr.df) %in% c("name","pop","uPop", "tPopFac", "tuPopFac"),]
popSums <- pops.df %>%
  group_by(name, tPopFac, tuPopFac) %>%
  summarise_all(funs(sum))
popCounts <- pops.df %>%
  count(name)
rename(popCounts, tiles = n)
pops <- full_join(popSums, popCounts, by="name", copy=FALSE, suffix=c(".sums", ".counts") )

tiles <- full_join(popCounts, uPopCounts, by="name", copy=FALSE, suffix=c(".pops", ".uPops"))
tiles

allPops <- full_join(popSums, uPopSums, by="name", copy=FALSE, suffix=c(".pops", ".uPops") )
all <- full_join(allPops, tiles, by="name", copy=FALSE)

all$pDensity = all$pop.pops / all$n.pops
all$uDensity = all$uPop.uPops / all$n.uPops
all$tratio <- (all$n.pops / 81428.0) * 100
all

for (fac in unique(terr.df$territories)) {
  dev.new()
  print(ggplot(terr.df[terr.df$territories==fac,]) +
    geom_histogram(mapping = aes(x=ratfac), stat="count") +
      labs(title = fac)
    )
}

popquery <- "MATCH (m:MembershipFact)-[:MEMBER]-(s:State)-[o]-(t:Territory{year:1816})-[:INCLUDES]->(h:Tile)-[:SIM_POPULATION]-(pf:PopulationFact)-[:DURING{year:1816}]-(:Year{name:'1816'}),
  (s)-[:POPULATION]-(f:PopulationFact)-[:DURING]-(:Year{name:'1816'})  WHERE t.cowcode = s.cowcode AND (m.from.year <= 1816 OR m.from.year IS NULL)
WITH t, f.value AS pop, count(h) AS tot
MATCH (t)-[:INCLUDES]->(h:Tile) WHERE h.population > 1000
WITH t, tot, pop, count(h) AS popk
OPTIONAL MATCH (t)-[:INCLUDES]->(h:Tile) WHERE h.population > 10000
WITH t, tot, pop, popk, count(h) AS pop10k
OPTIONAL MATCH (t)-[:INCLUDES]->(h:Tile) WHERE h.population > 100000
WITH t, tot, pop, popk, pop10k, count(h) AS pop100k
OPTIONAL MATCH (t)-[:INCLUDES]->(h:Tile) WHERE h.population > 1000000
RETURN t.name, tot, popk, pop10k, pop100k, count(h) AS popm, pop"

pop.df <- cypher(graph, popquery)
tpop.df <- pop.df[,1:6]

pop.df$rat <- pop.df$pop / pop.df$tot
pop.df$ratk <- pop.df$pop / pop.df$popk
pop.df$rat10k <- pop.df$pop / pop.df$pop10k
pop.df$rat100k <- pop.df$pop / pop.df$pop100k
summary(pop.df$rat)
summary(pop.df$ratk)
summary(pop.df$rat10k)
summary(pop.df$rat100k)


tall.pop.df <- melt(tpop.df, id.vars="t.name")
ggplot(tall.pop.df, aes(value, t.name,  col=variable) ) +
  scale_x_log10() +
  geom_jitter(width = 0, height = .5, alpha = .5)


ggplot(tall.pop.df, aes(y=value, x=t.name, fill=variable)) +
  scale_y_log10() +
  coord_trans(y = "pseudo_log") +
  coord_flip() +
  geom_bar(position = "dodge", stat="identity")


getPalette = colorRampPalette(brewer.pal(9, "Paired"))

fillColors = getPalette(228)
fillColors
