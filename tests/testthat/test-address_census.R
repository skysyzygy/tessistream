withr::local_package("checkmate")
withr::local_package("mockery")

# census_variables --------------------------------------------------------
census_variables <- census_variables()

test_that("census_variables identifies variables for each year and type", {
  years <- c(2000,seq(2009,2021))
  types <- c("race","sex_and_age","income")

  expect_named(census_variables,c("year","type","variable","dataset","label","age","sex","race","measure","concept"),
               ignore.order = TRUE)

  expect_false(census_variables[expand.grid(year=years,type=types),any(is.na(label)),on=c("year","type")])
})

test_that("census_variables identifies a sane number of variables for each acs year and type", {
  variables <- census_variables[!dataset %in% c("sf1","sf3")]
  years <- variables[,unique(year)]
  expect_equal(variables[type == "income",.N,by="year"]$N,rep(2,length(years)))
  expect_equal(variables[type == "race",.N,by="year"]$N,rep(6,length(years)))
  expect_equal(variables[type == "sex_and_age" & is.na(age),.N,by="year"]$N,rep(2,length(years)))
  expect_equal(variables[type == "sex_and_age" & !is.na(age),.N,by="year"]$N,rep(13,length(years)))
})

test_that("census_variables identifies a sane number of variables for each decennial year and type", {
  variables <- census_variables[dataset %in% c("sf1","sf3")]
  years <- variables[,unique(year)]
  expect_equal(variables[type == "income",.N,by="year"]$N,1)
  expect_equal(variables[type == "race",.N,by="year"]$N,rep(6,length(years)))
  expect_equal(variables[type == "sex_and_age" & is.na(age),.N,by="year"]$N,rep(2,length(years)))
  expect_equal(variables[type == "sex_and_age" & !is.na(age),.N,by="year"]$N,rep(46,length(years)))
})

test_that("census_variables categorizes each variable", {
  expect_false(census_variables[type == "sex_and_age",any(is.na(age) & is.na(sex))])
  expect_false(census_variables[type == "race",any(is.na(race))])
  expect_false(census_variables[type == "income",any(is.na(measure))])
})


# census_get_data ---------------------------------------------------------

test_that("census_get_data queries get_acs or get_decennial, once per state", {
  get_acs <- mock(data.table(), cycle = TRUE)
  get_decennial <- mock(data.table(), cycle = TRUE)
  stub(census_get_data,"get_acs",get_acs)
  stub(census_get_data,"get_decennial",get_decennial)
  stub(census_get_data,"furrr::future_map",purrr::map)
  num_fips_codes <- length(unique(tidycensus::fips_codes$state_code))

  census_get_data(2000,"sf1","first_variable")
  expect_length(mock_args(get_acs),0)
  expect_length(mock_args(get_decennial),num_fips_codes)
  census_get_data(2000,"acs5/profile","first_variable")
  expect_length(mock_args(get_acs),num_fips_codes)

})

test_that("census_get_data_all calls census_get_data once per combination of year and dataset", {
  census_get_data <- mock(data.table(), cycle = TRUE)
  stub(census_get_data_all,"census_get_data",census_get_data)

  census_get_data_all(census_variables)
  expect_length(mock_args(census_get_data), nrow(census_variables[,1,by=c("year","dataset")]))
})

# census_features ---------------------------------------------------------

.census_data <- DBI::dbReadTable(DBI::dbConnect(RSQLite::SQLite(), rprojroot::find_testthat_root_file("census_data.sqlite")), "address_census") %>% collect %>% setDT
stub(census_data, "address_cache_chunked", .census_data)

test_that("census_race_features returns all race/ethnicity data", {
  stub(census_race_features, "census_data", census_data)
  stub(census_race_features, "census_variables", census_variables)
  features <- census_race_features()

  expect_named(features,c("year","GEOID","value","moe","feature"),ignore.order = TRUE)
  # all features
  expect_names(unique(features$feature),
               permutation.of = c("White","Black or African American","American Indian and Alaska Native","Asian","Native Hawaiian and Other Pacific Islander","Other"))
  # all years
  expect_equal(unique(features$year),unique(.census_data$year))
  # low missingness
  expect_lt(features[,.(value=sum(value)),by=c("GEOID","year")][is.na(value) | value == 0,.N],nrow(features)*.01)
})

test_that("census_sex_features returns all sex data", {
  stub(census_sex_features, "census_data", census_data)
  stub(census_sex_features, "census_variables", census_variables)

  features <- census_sex_features()

  expect_named(features,c("year","GEOID","value","moe","feature"),ignore.order = TRUE)
  # all features
  expect_names(unique(features$feature), permutation.of = c("Male","Female"))
  # all years
  expect_equal(unique(features$year),unique(.census_data$year))
  # low missingness
  expect_lt(features[,.(value=sum(value)),by=c("GEOID","year")][is.na(value) | value == 0,.N],nrow(features)*.01)
})

age_features <- c("Under 5 years",
                  "5 to 9 years",
                  "10 to 14 years",
                  "15 to 19 years",
                  "20 to 24 years",
                  "25 to 34 years",
                  "35 to 44 years",
                  "45 to 54 years",
                  "55 to 59 years",
                  "60 to 64 years",
                  "65 to 74 years",
                  "75 to 84 years",
                  "85 years and over")

test_that("census_age_features returns all age data", {
  stub(census_age_features, "census_data", census_data)
  stub(census_age_features, "census_variables", census_variables)

  features <- census_age_features()

  expect_named(features,c("year","GEOID","value","moe","feature"),ignore.order = TRUE)
  expect_names(unique(features$feature), permutation.of = age_features)
  # all years
  expect_equal(unique(features$year),unique(.census_data$year))
  # low missingness
  expect_lt(features[,.(value=sum(value)),by=c("GEOID","year")][is.na(value) | value == 0,.N],nrow(features)*.01)

})

test_that("census_age_features combines age data to make consistent features", {
  census_data <- rbind(data.table(year = 2020, GEOID = "NYC", value = 0, dataset = "acs5/profile", moe = NA, estimate = NA, age = age_features),
                       data.table(year = 2010, GEOID = "NYC", value = 1, dataset = "sf1", moe = NA, estimate = NA, age = paste(seq(0,100),"years")))

  stub(census_age_features, "census_data", census_data)
  stub(census_age_features, "census_variables", census_variables)

  features <- census_age_features()

  expect_equal(features[year == 2010,value], c(5,5,5,5,5,10,10,10,5,5,10,10,16))

})

test_that("census_income_features returns all income data", {
  stub(census_income_features, "census_data", census_data)
  stub(census_income_features, "census_variables", census_variables)
  features <- census_income_features()

  expect_named(features,c("year","GEOID","value","moe","feature"),ignore.order = TRUE)
  expect_names(unique(features$feature), permutation.of = paste(c("Median","Mean"),"Income"))
  # all years
  expect_equal(unique(features$year),unique(.census_data$year))
  # low missingness
  expect_lt(features[,.(value=sum(value)),by=c("GEOID","year")][is.na(value) | value == 0,.N],nrow(features)*.02)

})

test_that("census_features returns a data.table with all features snake_cased", {
  stub(census_race_features, "census_data", census_data)
  stub(census_race_features, "census_variables", census_variables)
  stub(census_sex_features, "census_data", census_data)
  stub(census_sex_features, "census_variables", census_variables)
  stub(census_age_features, "census_data", census_data)
  stub(census_age_features, "census_variables", census_variables)
  stub(census_income_features, "census_data", census_data)
  stub(census_income_features, "census_variables", census_variables)

  stub(census_features, "census_race_features", census_race_features)
  stub(census_features, "census_sex_features", census_sex_features)
  stub(census_features, "census_age_features", census_age_features)
  stub(census_features, "census_income_features", census_income_features)

  census_features <- census_features()

  expect_named(census_features,ignore.order = T,
               c(paste("address",
                     c('under_5_years', '10_to_14_years', '15_to_19_years', '20_to_24_years', '25_to_34_years', '35_to_44_years', '45_to_54_years',
                     '55_to_59_years', '5_to_9_years', '60_to_64_years', '65_to_74_years', '75_to_84_years', '85_years_and_over',
                     'american_indian_and_alaska_native', 'asian', 'black_or_african_american', 'native_hawaiian_and_other_pacific_islander', 'other', 'white',
                     'female', 'male', 'mean_income', 'median_income'),
                     "level",sep="_"),
                 'year', 'GEOID'))

  expect_data_table(census_features)

})

test_that("census_*_features return features whose sums are (mostly) consistent across GEOID and year", {
  stub(census_race_features, "census_data", census_data)
  stub(census_race_features, "census_variables", census_variables)
  stub(census_sex_features, "census_data", census_data)
  stub(census_sex_features, "census_variables", census_variables)
  stub(census_age_features, "census_data", census_data)
  stub(census_age_features, "census_variables", census_variables)

  census_race_features <- census_race_features()[,sum(value,na.rm=T),by=c("GEOID","year")]
  census_sex_features <- census_sex_features()[,sum(value,na.rm=T),by=c("GEOID","year")]
  census_age_features <- census_age_features()[,sum(value,na.rm=T),by=c("GEOID","year")]

  expect_lt(merge(census_race_features,census_sex_features,by=c("GEOID","year"))[,error := abs(V1.x-V1.y)/pmax(V1.x,V1.y)][error>.1,.N],
            nrow(census_race_features)*.1)
  expect_lt(merge(census_race_features,census_age_features,by=c("GEOID","year"))[,error := abs(V1.x-V1.y)/pmax(V1.x,V1.y)][error>.1,.N],
            nrow(census_race_features)*.1)
  # sex and age should be internally consistent
  expect_equal(merge(census_sex_features,census_age_features,by=c("GEOID","year"))[,error := abs(V1.x-V1.y)/pmax(V1.x,V1.y)][error>0,.N],0)

})

# address_census ----------------------------------------------------------

test_that("address_census adds census data to address_stream", {
  tessilake::local_cache_dirs()

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave","30 Churchill Pl"),
    street2 = NA,
    city = c("Brooklyn","London"),
    state = c("NY",NA),
    country = c("USA","UK"),
    postal_code = c("11217","E14 5EU"),
    timestamp = lubridate::now()
  )

  address_geocode <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))[address_stream,on = address_cols]
  stub(address_reverse_census,"address_geocode",address_geocode)
  stub(address_census,"address_reverse_census",address_reverse_census)
  stub(address_census, "census_features", readRDS(rprojroot::find_testthat_root_file("census_features.Rds")))

  expect_message(res <- address_census(address_stream))
  expect_named(res,ignore.order = T,
               c("address_under_5_years_level", "address_10_to_14_years_level",  "address_15_to_19_years_level", "address_20_to_24_years_level",
                 "address_25_to_34_years_level", "address_35_to_44_years_level",  "address_45_to_54_years_level", "address_55_to_59_years_level",
                 "address_5_to_9_years_level", "address_60_to_64_years_level",  "address_65_to_74_years_level", "address_75_to_84_years_level",
                 "address_85_years_and_over_level",
                 "address_american_indian_and_alaska_native_level",  "address_asian_level", "address_black_or_african_american_level",
                 "address_native_hawaiian_and_other_pacific_islander_level", "address_other_level",  "address_white_level",
                 "address_female_level", "address_male_level",
                 "address_mean_income_level", "address_median_income_level",
                 address_cols,"timestamp"))

})

test_that("address_census converts demographic labels to percentages", {
  tessilake::local_cache_dirs()
  withr::local_package("dplyr")

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave","30 Churchill Pl"),
    street2 = NA,
    city = c("Brooklyn","London"),
    state = c("NY",NA),
    country = c("USA","UK"),
    postal_code = c("11217","E14 5EU"),
    timestamp = lubridate::now()
  )

  address_geocode <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))[address_stream,on = address_cols]
  stub(address_reverse_census,"address_geocode",address_geocode)
  stub(address_census,"address_reverse_census",address_reverse_census)
  stub(address_census, "census_features", readRDS(rprojroot::find_testthat_root_file("census_features.Rds")))

  expect_message(res <- address_census(address_stream))

  expect_true(between(sum(res[,c("address_under_5_years_level", "address_10_to_14_years_level",  "address_15_to_19_years_level", "address_20_to_24_years_level",
                      "address_25_to_34_years_level", "address_35_to_44_years_level",  "address_45_to_54_years_level", "address_55_to_59_years_level",
                      "address_5_to_9_years_level", "address_60_to_64_years_level",  "address_65_to_74_years_level", "address_75_to_84_years_level",
                      "address_85_years_and_over_level"),with=F],na.rm=T),.9,1.1))
  expect_true(between(sum(res[,c("address_american_indian_and_alaska_native_level",  "address_asian_level", "address_black_or_african_american_level",
                          "address_native_hawaiian_and_other_pacific_islander_level", "address_other_level",  "address_white_level"),with=F],na.rm=T),.9,1.1))
  expect_true(between(sum(res[,c("address_female_level", "address_male_level"),with=F],na.rm=T),.9,1.1))
})

test_that("address_census replaces 0s in income columns with NA", {
  tessilake::local_cache_dirs()

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave","30 Churchill Pl"),
    street2 = NA,
    city = c("Brooklyn","London"),
    state = c("NY",NA),
    country = c("USA","UK"),
    postal_code = c("11217","E14 5EU"),
    timestamp = lubridate::now()
  )

  address_geocode <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))[address_stream,on = address_cols]
  stub(address_reverse_census,"address_geocode",address_geocode)
  stub(address_census,"address_reverse_census",address_reverse_census)
  census_features <- readRDS(rprojroot::find_testthat_root_file("census_features.Rds"))
  census_features[,`:=`(address_median_income_level = 0, address_mean_income_level = 0)]
  stub(address_census, "census_features", census_features)

  expect_message(res <- address_census(address_stream))

  expect_equal(res$address_median_income_level,rep(NA_real_,2))
  expect_equal(res$address_mean_income_level,rep(NA_real_,2))

})
