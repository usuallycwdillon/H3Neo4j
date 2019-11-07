## Frequency of Wars Onset by week, Weeks 50-10224 (a little before 1816 until ~ 2010)
##
library("RNeo4j")
library("naniar")
library("ggplot2")
library("imputeTS")
library("dplyr")
library("tidyr")
library("data.table")

## Configure a connection to the graph database
graph <- startGraph("http://192.168.1.94:7474/db/data",
                    username="neo4j",
                    password="george")

## Now, let's look for change in mean probability of war initiation over time
##

# Create tibble of all zeros for all possible weeks
allWeeks <- c(50:10224)
tb.all <- tibble(week=allWeeks, inWars=0, onWars=0, inStates=0, onStates=0)

## All wars and Militarized Interstate Dispute initiations by the week initiated
query = "MATCH (fw:Week)-[:FROM_WEEK]-(f:WarParticipationFact)-[:PARTICIPATED_IN]-(w:War), (f)-[p:PARTICIPATED{initiated:true}]-(s:State)
WITH collect({week:fw.stepNumber, wars:w, part:f, init:p}) AS rows
MATCH (fw:Week)-[:FROM_WEEK]-(f:DisputeParticipationFact{originatedDistpute:true})-[:PARTICIPATED_IN]-(d:Dispute), (f)-[p:PARTICIPATED]-(s:State)
WITH rows + collect({week:fw.stepNumber, wars:d, part:f, init:p}) AS allRows
UNWIND allRows AS r
WITH r.week as week, r.wars as wars, r.part as part, r.init as init
RETURN DISTINCT week, count(DISTINCT wars) as wars, count(part) as states ORDER BY week"

df.inits <- cypher(graph, query)
cols <- c("week", "inWars", "inStates")
colnames(df.inits) <- cols
# df.inits
tb.inits <- as_tibble(df.inits)


query = "MATCH (fw:Week)-[:FROM_WEEK]-(f:WarParticipationFact)-[:PARTICIPATED_IN]-(w:War), (f)-[p:PARTICIPATED]-(s:State)
WITH collect({week:fw.stepNumber, wars:w, part:f}) AS rows
MATCH (fw:Week)-[:FROM_WEEK]-(f:DisputeParticipationFact)-[:PARTICIPATED_IN]-(d:Dispute), (f)-[p:PARTICIPATED]-(s:State)
WITH rows + collect({week:fw.stepNumber, wars:d, part:f}) AS allRows
UNWIND allRows AS r
WITH r.week as week, r.wars as wars, r.part as part
RETURN DISTINCT week, count(DISTINCT wars) as wars, count(part) as states ORDER BY week"

df.onsets <- cypher(graph, query)
cols <- c("week", "onWars", "onStates")
colnames(df.onsets) <- cols
# df.onsets
tb.onsets <- as_tibble(df.onsets)

#  copy the real data into the table
tb.all$inWars[match(tb.inits$week, tb.all$week)] <- tb.inits$inWars
tb.all$inStates[match(tb.inits$week, tb.all$week)] <- tb.inits$inStates
tb.all$onWars[match(tb.onsets$week, tb.all$week)] <- tb.onsets$onWars
tb.all$onStates[match(tb.onsets$week, tb.all$week)] <- tb.onsets$onStates

inWars <- tb.all %>% group_by(inWars) %>% count()
inStates <- tb.all %>% group_by(inStates) %>% count()
onWars <-tb.all %>% group_by(onWars) %>% count()
onStates <- tb.all %>% group_by(onStates) %>% count()


# Fit some models against this data - compare power-law, poisson (we expect this to follow a poisson, bc of Richardson's 1944 paper), and lognormal. ...it's definitely NOT poisson, tho.
owm1 <- displ$new(onWars$n)
owm2 <- dispois$new(onWars$n)
owm3 <- dislnorm$new(onWars$n)
owm1$setPars(estimate_pars(owm1))
owm1$setXmin(estimate_xmin(owm1))
owm2$setXmin(owm1$getXmin())
owm3$setXmin(owm1$getXmin())
owm2$setPars(estimate_pars(owm2))
owm3$setPars(estimate_pars(owm3))
plot(owm1, ylab='cdf', xlab='Frequency of weeks with counts of war participation onset')
lines(owm1, col=1, lty=1)
lines(owm2, col=2, lty=1)
lines(owm3, col=3, lty=1)
compare_distributions(owm3, owm1)
compare_distributions(owm2, owm1) # We can reject poisson is as close as power-law
compare_distributions(owm2, owm3) # We can reject posson over lognormal (but marginally less confidently)

iwm1 <- displ$new(inWars$n)
iwm2 <- dispois$new(inWars$n)
iwm3 <- dislnorm$new(inWars$n)
iwm1$setPars(estimate_pars(iwm1))
iwm1$setXmin(estimate_xmin(iwm1))
iwm2$setPars(estimate_pars(iwm2))
iwm2$setXmin(owm1$getXmin())
iwm3$setPars(estimate_pars(iwm3))
iwm3$setXmin(owm1$getXmin())
plot(iwm1, ylab='cdf', xlab='Frequency of weeks with counts of war participation initiation')
lines(iwm1, col=1, lty=1)
lines(iwm2, col=2, lty=1)
lines(iwm3, col=3, lty=1)
compare_distributions(iwm3, iwm1)
compare_distributions(iwm2, iwm1) # We can reject poisson over power-law
compare_distributions(iwm2, iwm3) # We can reject poisson over lognormal (more confidently than pl)


osm1 <- displ$new(onStates$n)
osm2 <- dispois$new(onStates$n)
osm3 <- dislnorm$new(onStates$n)
osm1$setPars(estimate_pars(osm1))
osm1$setXmin(estimate_xmin(iwm1))
osm2$setPars(estimate_pars(osm2))
osm2$setXmin(osm1$getXmin())
osm3$setPars(estimate_pars(osm3))
osm3$setXmin(osm1$getXmin())
plot(osm1, ylab='cdf', xlab='Frequency of weeks with counts of states war participation onset')
lines(osm1, col=1, lty=1)
lines(osm2, col=2, lty=1)
lines(osm3, col=3, lty=1)
compare_distributions(osm3, osm1)
compare_distributions(osm2, osm1)
compare_distributions(osm2, osm3)


ism1 <- displ$new(inStates$n)
ism2 <- dispois$new(inStates$n)
ism3 <- dislnorm$new(inStates$n)
ism1$setPars(estimate_pars(ism1))
ism1$setXmin(estimate_xmin(ism1))
ism2$setPars(estimate_pars(ism2))
ism2$setXmin(ism1$getXmin())
ism3$setPars(estimate_pars(ism3))
ism3$setXmin(ism1$getXmin())
plot(ism1, ylab='cdf', xlab='Frequency of weeks with counts of states war participation onset')
lines(ism1, col=1, lty=1)
lines(ism2, col=2, lty=1)
lines(ism3, col=3, lty=1)
compare_distributions(ism3, ism1)
compare_distributions(ism2, ism1)
compare_distributions(ism2, ism3)



