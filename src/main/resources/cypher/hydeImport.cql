CREATE (s:Source)
SET s.name = "Spatially-explicit Gross Cell Product (GCP) time series",
  s.version = toFloat(2017),
  s.published = date({year:2017}),
  s.validFrom = date({year:1850, month:1, day:1}),
  s.validUntil = date({year:2100, month:12, day:31}),
  s.citation = "Geiger, Tobias; Murakami, Daisuke; Frieler, Katja; Yamagata, Yoshiki (2017): Spatially-explicit Gross Cell Product (GCP) time series: past observations (1850-2000) harmonized with future projections according the Shared Socioeconomic Pathways (2010-2100). GFZ Data Services., url:http://doi.org/10.5880/pik.2017.007",
  s.overview = "We here provide spatially-explicit economic time series for Gross Cell Product (GCP) with global coverage in 10-year increments between 1850 and 2100 with a spatial resolution of 5 arcmin. GCP is based on a 1statistcal downscaling procedure that among other predictors uses national Gross Domestic Product (GDP) time series and gridded population estimates as input. ",
  s.purpose = "Historical estimates until 2000 are harmonized with future socio-economic projections from the Shared Socioeconomic Pathways (SSPs) according to SSP2 from 2010 onwards."
MERGE (s)-[:PROVIDES]->(d:Dataset{
  name:"GCP 1850-2100 at 2005 ppp",
  version:toFloat(2017.007),
  published:date({year:2017}),
  purpose:"Spatially-explicit Gross Cell Product decenial years 1850-2100.",
  url:"https://dataservices.gfz-potsdam.de/pik/showshort.php?id=escidoc:2740907",
  filename:"GCP_PPP_1850-2100.nc"
  })
MERGE (d)-[:INPUT_TO]->(c:Computation{filename:"geiger.R"})-[:COMPUTES]->(:Dataset{
  name:"Gross Cell Product 1850",
  note:"This data file is extracted and converted from Geiger, et al's original NETCDF4 formated dataset: GCP 1850-2100 at 2005 ppp",
  purpose:"Spatially-explicit Gross Cell Product for year 1850.",
  year:1850,
  filename:"gcp1850.csv"
})
MERGE (c)-[:COMTUTES]->(:Dataset{
  name:"Gross Cell Product 1880",
  note:"This data file is this author's converted and extracted data from Geiger, et al's GCP data set from their original NETCDF4 format",
  purpose:"Spatially-explicit Gross Cell Product for year 1880.",
  year:1880,
  filename:"gcp1880.csv"
})
MERGE (c)-[:COMTUTES]->(:Dataset{
  name:"Gross Cell Product 1914",
  note:"This data file is this author's converted and extracted data from Geiger, et al's GCP data set from their original NETCDF4 format; representing a weighted average between the decenials before and after.",
  purpose:"Spatially-explicit Gross Cell Product for year 1914.",
  year:1914,
  filename:"gcp1914.csv"
})
MERGE (c)-[:COMTUTES]->(:Dataset{
  name:"Gross Cell Product 1938",
  note:"This data file is this author's converted and extracted data from Geiger, et al's GCP data set from their original NETCDF4 format; representing a weighted average between the decenials before and after.",
  purpose:"Spatially-explicit Gross Cell Product for year 1938.",
  year:1938,
  filename:"gcp1938.csv"
})
MERGE (c)-[:COMTUTES]->(:Dataset{
  name:"Gross Cell Product 1945",
  note:"This data file is this author's converted and extracted data from Geiger, et al's GCP data set from their original NETCDF4 format; representing a weighted average between the decenials before and after.",
  purpose:"Spatially-explicit Gross Cell Product for year 1945.",
  year:1945,
  filename:"gcp1945.csv"
})
MERGE (c)-[:COMTUTES]->(:Dataset{
  name:"Gross Cell Product 1994",
  note:"This data file is this author's converted and extracted data from Geiger, et al's GCP data set from their original NETCDF4 format; representing a weighted average between the decenials before and after.",
  purpose:"Spatially-explicit Gross Cell Product for year 1994.",
  year:1994,
  filename:"gcp1994.csv"
});
MATCH (d:Dataset{name:"Gross Cell Product 1850"})
WITH d
CALL wog.writeHydeFacts(d, "1850", "gtp", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"Gross Cell Product 1880"})
WITH d
CALL wog.writeHydeFacts(d, "1880", "gtp", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"Gross Cell Product 1914"})
WITH d
CALL wog.writeHydeFacts(d, "1914", "gtp", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"Gross Cell Product 1938"})
WITH d
CALL wog.writeHydeFacts(d, "1938", "gtp", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"Gross Cell Product 1945"})
WITH d
CALL wog.writeHydeFacts(d, "1945", "gtp", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"Gross Cell Product 1994"})
WITH d
CALL wog.writeHydeFacts(d, "1994", "gtp", true) YIELD value RETURN value;

CREATE (s:Source)
SET s.name = "Anthropogenic land use estimates for the Holocene",
  s.version = toFloat(3.2),
  s.published = date({year:2017, month:9, day:12}),
  s.validFrom = date({year:-10000}),
  s.validUntil = date({year:2014, month:12, day:31}),
  s.citation = "Klein Goldewijk, K., A. Beusen, J.Doelman and E. Stehfest (2017), Anthropogenic land use estimates for the Holocene; HYDE 3.2, Earth System Science Data, 9, 927-953.",
  s.overview = "HYDE is an internally consistent combination of updated historical population estimates and land use. Population is represented by maps of total, urban, rural population and population density as well as built-up area. The period covered is 10 000 BCE to 2015 CE. Spatial resolution is 5 arc minutes (approx. 85 km2 at the equator), the files are in Arcmap asciigrid format.",
  s.purpose = "This HYDE version 3.2.1 replaces former 2017_beta_releases 3.2.000 (from august 2016) and 3.2.001 (from october 2016). Major updates concern use of new FAO data until the year 2014 (instead of 2012), improved computations of (physical) Rice areas, and an adjustment of the cropland area for India before 1000 CE."
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1810",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1810.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1810,
  filename:"1810AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1820",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1820.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1820,
  filename:"1820AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1850",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1850.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1850,
  filename:"1850AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1880",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1880.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1880,
  filename:"1880AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1910",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1910.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1910,
  filename:"1910AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1920",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1920.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1920,
  filename:"1920AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1930",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1930.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1930,
  filename:"1930AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1940",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1940.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1940,
  filename:"1940AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1950",
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1950.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  year:1950,
  filename:"1950AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 1990",
  year:1990,
  version:toFloat(3.2),
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 1990.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  filename:"1990AD_pop.zip"
  })
MERGE (s)-[:PROVIDES]->(:Dataset{
  name:"HYDE Population Estimates 2000",
  version:toFloat(3.2),
  year:2000,
  published:date({year:2017, month:9, day:12}),
  purpose:"Represent in maps the total, urban, rural population and population density as well as built-up area in 2000.",
  url:"ftp://ftp.pbl.nl/hyde/hyde3.2/",
  filename:"2000AD_pop.zip"
  });
MATCH (d1:Dataset{name:"HYDE Population Estimates 1810"}), (d2:Dataset{name:"HYDE  Population Estimates 1820"})
MERGE (d1)-[:INPUT_TO]->(c:Computation{filename:"goldewijk.R"})
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1816",
  year:1816,
  note:"This data file represents straight line interpolated values between 1810 and 1820 for 1816",
  purpose:"Provides reasonable estimates for global population distribution in 1816.",
  filename:"popc1816.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1816",
  year:1816,
  note:"This data file represents straight line interpolated values between 1810 and 1820 for 1816",
  purpose:"Provides reasonable estimates for global urban population distribution in 1816.",
  filename:"urbc1816.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1816",
  year:1816,
  note:"This data file represents straight line interpolated values between 1810 and 1820 for 1816",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1816.",
  filename:"uopp1816.csv"
});
MATCH (d1:Dataset{name:"HYDE  Population Estimates 1910"}), (d2:Dataset{name:"HYDE  Population Estimates 1920"}), (c:Computation{filename:"goldewijk.R"})
MERGE (d1)-[:INPUT_TO]->(c)
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1914",
  year:1914,
  note:"This data file represents straight line interpolated values between 1910 and 1920 for 1914",
  purpose:"Provides reasonable estimates for global population distribution in 1914.",
  filename:"popc1914.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1914",
  year:1914,
  note:"This data file represents straight line interpolated values between 1910 and 1920 for 1914",
  purpose:"Provides reasonable estimates for global urban population distribution in 1914.",
  filename:"urbc1914.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1914",
  year:1914,
  note:"This data file represents straight line interpolated values between 1910 and 1920 for 1914",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1914.",
  filename:"uopp1914.csv"
});
MATCH (d1:Dataset{name:"HYDE  Population Estimates 1930"}), (d2:Dataset{name:"HYDE  Population Estimates 1940"}), (c:Computation{filename:"goldewijk.R"})
MERGE (d1)-[:INPUT_TO]->(c)
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1938",
  year:1938,
  note:"This data file represents straight line interpolated values between 1930 and 1940 for 1938",
  purpose:"Provides reasonable estimates for global population distribution in 1938.",
  filename:"popc1938.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1938",
  year:1938,
  note:"This data file represents straight line interpolated values between 1930 and 1940 for 1938",
  purpose:"Provides reasonable estimates for global urban population distribution in 1938.",
  filename:"urbc1938.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1938",
  year:1938,
  note:"This data file represents straight line interpolated values between 1930 and 1940 for 1938",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1938.",
  filename:"uopp1938.csv"
});
MATCH (d1:Dataset{name:"HYDE  Population Estimates 1940"}), (d2:Dataset{name:"HYDE  Population Estimates 1950"}), (c:Computation{filename:"goldewijk.R"})
MERGE (d1)-[:INPUT_TO]->(c)
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1945",
  year:1945,
  note:"This data file represents straight line interpolated values between 1940 and 1950 for 1945",
  purpose:"Provides reasonable estimates for global population distribution in 1945.",
  filename:"popc1945.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1945",
  year:1945,
  note:"This data file represents straight line interpolated values between 1940 and 1950 for 1945",
  purpose:"Provides reasonable estimates for global urban population distribution in 1945.",
  filename:"urbc1945.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1945",
  year:1945,
  note:"This data file represents straight line interpolated values between 1940 and 1950 for 1945",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1945.",
  filename:"uopp1945.csv"
});
MATCH (d1:Dataset{name:"HYDE  Population Estimates 1990"}), (d2:Dataset{name:"HYDE  Population Estimates 2000"}), (c:Computation{filename:"goldewijk.R"})
MERGE (d1)-[:INPUT_TO]->(c)
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1994",
  year:1994,
  note:"This data file represents straight line interpolated values between 1990 and 2000 for 1994",
  purpose:"Provides reasonable estimates for global population distribution in 1994.",
  filename:"popc1994.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1994",
  year:1994,
  note:"This data file represents straight line interpolated values between 1990 and 2000 for 1994",
  purpose:"Provides reasonable estimates for global urban population distribution in 1994.",
  filename:"urbc1994.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1994",
  year:1994,
  note:"This data file represents straight line interpolated values between 1990 and 2000 for 1994",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1994.",
  filename:"uopp1994.csv"
});
MATCH (d1:Dataset{name:"HYDE Population Estimates 1850"}), (c:Computation{filename:"goldewijk.R"})
MERGE (d1)-[:INPUT_TO]->(c)
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1850",
  year:1850,
  note:"This data file represents converted data from 1850",
  purpose:"Provides reasonable estimates for global population distribution in 1850.",
  filename:"popc1850.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1850",
  year:1850,
  note:"This data file represents converted data from 1850",
  purpose:"Provides reasonable estimates for global urban population distribution in 1850.",
  filename:"urbc1850.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1850",
  year:1850,
  note:"This data file represents converted data from 1850",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1850.",
  filename:"uopp1850.csv"
});
MATCH (d1:Dataset{name:"HYDE Population Estimates 1880"}), (c:Computation{filename:"goldewijk.R"})
MERGE (d1)-[:INPUT_TO]->(c)
MERGE (d2)-[:INPUT_TO]->(c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Population Estimates 1880",
  year:1880,
  note:"This data file represents converted data from 1880",
  purpose:"Provides reasonable estimates for global population distribution in 1880.",
  filename:"popc1880.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Urban Population Estimates 1880",
  year:1880,
  note:"This data file represents converted data from 1880",
  purpose:"Provides reasonable estimates for global urban population distribution in 1880.",
  filename:"urbc1880.csv"
})
MERGE (c)-[:COMPUTES]->(:Dataset{
  name:"HYDE Built-up Area Estimates 1880",
  year:1880,
  note:"This data file represents converted data from 1880",
  purpose:"Provides reasonable estimates for global built-up area distribution in 1880.",
  filename:"uopp1880.csv"
});
MATCH (d:Dataset{name:"HYDE Population Estimates 1816"})
WITH d
CALL wog.writeHydeFacts(d, "1816", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1850"})
WITH d
CALL wog.writeHydeFacts(d, "1850", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1880"})
WITH d
CALL wog.writeHydeFacts(d, "1880", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1914"})
WITH d
CALL wog.writeHydeFacts(d, "1914", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1938"})
WITH d
CALL wog.writeHydeFacts(d, "1938", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1945"})
WITH d
CALL wog.writeHydeFacts(d, "1945", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1994"})
WITH d
CALL wog.writeHydeFacts(d, "1994", "population", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Population Estimates 1816"})
WITH d
CALL wog.writeHydeFacts(d, "1816", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1850"})
WITH d
CALL wog.writeHydeFacts(d, "1850", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1880"})
WITH d
CALL wog.writeHydeFacts(d, "1880", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1914"})
WITH d
CALL wog.writeHydeFacts(d, "1914", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1938"})
WITH d
CALL wog.writeHydeFacts(d, "1938", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1945"})
WITH d
CALL wog.writeHydeFacts(d, "1945", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1994"})
WITH d
CALL wog.writeHydeFacts(d, "1994", "urbanPopulation", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Urban Population Estimates 1816"})
WITH d
CALL wog.writeHydeFacts(d, "1816", "builtArea", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Built-up Area Estimates 1850"})
WITH d
CALL wog.writeHydeFacts(d, "1850", "builtArea", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Built-up Area Estimates 1880"})
WITH d
CALL wog.writeHydeFacts(d, "1880", "builtArea", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Built-up Area Estimates 1914"})
WITH d
CALL wog.writeHydeFacts(d, "1914", "builtArea", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Built-up Area Estimates 1938"})
WITH d
CALL wog.writeHydeFacts(d, "1938", "builtArea", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Built-up Area Estimates 1945"})
WITH d
CALL wog.writeHydeFacts(d, "1945", "builtArea", true) YIELD value RETURN value;
MATCH (d:Dataset{name:"HYDE Built-up Area Estimates 1994"})
WITH d
CALL wog.writeHydeFacts(d, "1994", "builtArea", true) YIELD value RETURN value;