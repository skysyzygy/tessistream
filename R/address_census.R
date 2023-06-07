#' census_variables
#'
#' Get a collection of basic census variables for sex, age, race, and income.
#'
#' @return data.table of census variables
#'
#' @importFrom tidycensus load_variables
#' @importFrom lubridate year
#' @importFrom purrr map flatten discard
census_variables <- function() {
  load_variables <- function(year) {
    map(c("acs5/profile","sf1","sf3"),
               ~try(
                 cbind(tidycensus::load_variables(year, ., cache = TRUE),
                       dataset = .,
                       year = year),
                 silent = TRUE)
    )
  }

  variables <- map(seq(1990,year(Sys.Date())), load_variables) %>%
    map(discard,~!is.data.frame(.)) %>%
    flatten() %>% rbindlist(fill=T)

  variables[grepl("^(number|estimate|total)",label, ignore.case = TRUE) & (
    grepl("sex and age",label,ignore.case=T) | grepl("^sex by age( \\[49|$)",concept,ignore.case=T)) &
      grepl("!!\\d+ (to|and) \\d+ years$|!!(18|19|20|21) years$|Under 5 years|85 years and over|(fe)?male$", label, ignore.case = T, perl = TRUE) &
      grepl("^(DP\\d|P\\d)", name),
    type := "sex_and_age"]
  variables[type == "sex_and_age", `:=`(sex = stringr::str_match(label, stringr::regex("(?<sex>Male|Female)", ignore_case = TRUE))[,"sex"],
                                        age = stringr::str_match(label, stringr::regex("(!!|^)(?<age>[^!]*year[^!]*)$", ignore_case = TRUE))[,"age"])]
  variables[type == "sex_and_age" & is.na(sex) & is.na(age), type := NA]
  variables[type == "sex_and_age",I:=1:.N,by=list(year,dataset,age,tolower(sex))]
  # variables[type == "sex_and_age"] %>% View

  variables[grepl("^(number|estimate|total).+one race!![^!]+$", label, ignore.case = T), type := "race"]
  variables[type == "race", race := stringr::str_match(label, stringr::regex("(?<race>[^!]+?)( alone)?$", ignore_case = TRUE))[,"race"]]
  variables[type == "race", I:=1:.N, by=list(year,dataset, tolower(race))]
  # variables[type == "race"] %>% View

  variables[grepl("(median|mean) household income", label, ignore.case = T) &
              grepl("number|estimate|total", label, ignore.case = T), type := "income"]
  variables[type == "income", measure := stringr::str_match(label, stringr::regex("(?<measure>Mean|Median)", ignore_case = TRUE))[,"measure"]]
  variables[type == "income", I:=1:.N, by=list(year,dataset,measure)]
  # variables[type == "income"] %>% View

  variables <- variables[!is.na(type) & I==1]
  setnames(variables, "name", "variable")
  variables$I <- NULL
  variables[]

}

#' census_get_data
#'
#' Wrapper around [tidycensus::get_decennial] and [tidycensus::get_acs]
#'
#' @param year integer The year for which you are requesting data.
#' @param dataset character dataset to load from
#' @param variables character vector of variables to load
#'
#' @return data.table of census data
#' @importFrom tidycensus get_decennial get_acs
#' @importFrom checkmate assert_integerish
census_get_data <- function(year,dataset,variables) {
  assert_integerish(year,len = 1)
  assert_character(dataset, len = 1)
  assert_character(variables, min.len = 1, any.missing = FALSE)

  census_getter <- if (dataset %in% c("sf1","sf3")) {
    get_decennial
  } else {
    get_acs
  }

  census_data <- furrr::future_map(tidycensus::fips_codes[,"state_code"] %>% unique,
    ~try(census_getter(geography = "tract",
                  variables = variables,
                  year = year,
                  sumfile = dataset,
                  state = .,
                  cache_table = TRUE))
  )

  rbindlist(purrr::keep(census_data,is.data.frame),fill=T) %>%
    .[,`:=`(year = year,
            dataset = dataset)]
}

#' census_data
#'
#' Loads and caches census data
#'
#' @param census_variables data.table of years/datasets/variables to pull data for
#' @param ... additional parameters to pass on to [address_cache]
#'
#' @return data.table of all census data and variable info
#' @describeIn census_data Loads census data through cache
census_data <- function(census_variables, ...) {

  address_cache(census_variables, "address_census", census_get_data_all,
                key_cols = c("year","dataset","variable"),
                ...) %>%
    merge(census_variables, by=c("year","dataset","variable"))

}

#' @describeIn census_data Loads census data (does not cache)
census_get_data_all <- function(census_variables) {
  assert_data_table(census_variables)
  assert_names(colnames(census_variables), must.include = c("year","dataset","variable"))

  split(census_variables,by=c("year","dataset")) %>%
    purrr::map(~census_get_data(.$year[[1]],.$dataset[[1]],.$variable)) %>%
    rbindlist(fill = TRUE)

}

#' @describeIn census_features Prepares census race/ethnicity data
census_race_features <- function() {

  race_variables <- census_variables()[type == "race" & (year != 2010 | dataset == "sf1")]
  race_features <- census_data(race_variables) %>% .[,.(year, GEOID, value = dplyr::coalesce(value,estimate), moe, feature = race)] %>%
    .[,feature := ifelse(grepl("some other",feature,ignore.case = TRUE),"Other",feature)]

}

#' @describeIn census_features Prepares census sex data
census_sex_features <- function() {

  sex_variables <- census_variables()[type == "sex_and_age" & sex %in% c("Female","Male") & is.na(age) & (year != 2010 | dataset == "sf1")]
  sex_features <- census_data(sex_variables)[,value := coalesce(value,estimate)] %>%
    .[,.(value = sum(value,na.rm=T),moe = sum(moe,na.rm = TRUE)), by = list(year,GEOID,feature = sex)]

}

#' @describeIn census_features Prepares census age data
census_age_features <- function() {

  age_variables <- census_variables()[type == "sex_and_age" & !is.na(age) & (year != 2010 | dataset == "sf1")]

  age_features <- census_data(age_variables)

  acs_variables <- age_variables[grepl("acs",dataset),.(age = unique(age))] %>%
    .[,c("start","end") := stringr::str_match(age,"(Under|(?<start>\\d+)).+?(and|(?<end>\\d+))")[,c("start","end")] %>% as.data.frame %>% lapply(as.integer)]

  # Normalize ages
  age_features[!age %in% acs_variables$age, age := acs_variables[
    age_features[!age %in% acs_variables$age,.(start = stringr::str_match(age,"\\d+") %>% as.integer)],
    age, on = "start", roll = TRUE]]

  age_features[,value := coalesce(value,estimate)] %>%
    .[,.(value = sum(value,na.rm=T),moe=sum(moe,na.rm=T)), by = list(year,GEOID,feature = age)]

}

#' @describeIn census_features Prepares census income data
census_income_features <- function() {

  income_variables <- census_variables()[type == "income" & !is.na(measure)]
  income_features <- census_data(income_variables) %>% .[,.(year,GEOID,value = dplyr::coalesce(value,estimate),moe,feature = paste(measure,"Income"))]

}

#' census_features
#'
#' Prepare features from raw census data
#'
#' @return data.table of features extracted from census datasets
census_features <- function() {
  rbind(census_race_features(),
        census_sex_features(),
        census_age_features(),
        census_income_features()) %>%
    # snake_case them
    .[,feature := tolower(feature) %>% gsub("\\s+","_", .), by = "feature"] %>%
    data.table::dcast(year + GEOID ~ feature, fun.aggregate = sum, na.rm = TRUE, value.var = "value")

}

if(FALSE) {



  if (!exists("census_db")) {


    # add column names
    merge(census_db, variables, by.x = c("variable", "year"), by.y = c("name", "year"), all.x = T) %>%
      .[!is.na(label)] %>%
      .[, .(year, GEOID, estimate = coalesce(estimate, value), label, type)] %>%
      # cleanup and normalize columns
      .[type == "income" & grepl("Median", label), variable := "addressMedianIncomeLevel"] %>%
      # drop mean income for now...
      .[type != "income" | grepl("Median", label)] %>%
      # cleanup race labels
      .[type == "race", variable := paste0("address", gsub(" |Alone", "", stringr::str_to_title(label)), "Level")] %>%
      # add population totals
      rbind(.[type == "race", .(type = min(type), estimate = sum(estimate, na.rm = T), label = NA, variable = "addressTotalLevel"), by = c("GEOID", "year")]) %>%
      {
        rbind(
          # get median age from age brackets
          .[type == "sex_and_age" & year > 2000, .(age = stringr::str_extract(label, "\\d+") %>% as.integer() + 5, GEOID, year, estimate)] %>%
            setkey(year, GEOID, age) %>%
            .[, pct := cumsum(estimate) / sum(estimate, na.rm = T), by = c("year", "GEOID")] %>%
            .[pct > .5, .(estimate = age[1], type = "sex_and_age"), by = c("year", "GEOID")],
          .[type != "sex_and_age" | year == 2000],
          fill = T
        ) %>%
          .[type == "sex_and_age", variable := "addressMedianAgeLevel"]
      } -> census_db

    save(census_db, file = "census_db.RData")
  }

  zcta_crosswalk <- as.data.table(zcta_crosswalk)
  census_db_zcta <- merge(census_db, zcta_crosswalk[, .(GEOID = as.character(GEOID), ZCTA5)], by = "GEOID", all.x = T, allow.cartesian = T) %>%
    .[, .(mean = mean(estimate, na.rm = T), sum = sum(estimate, na.rm = T)), by = c("ZCTA5", "year", "variable")] %>%
    .[, .(ZCTA5, year, variable, estimate = if_else(grepl("Median", variable), mean, sum))] %>%
    .[is.nan(estimate), estimate := NA]

  dcast(census_db, year + GEOID ~ variable, value.var = "estimate") -> census_db_wide
  dcast(census_db_zcta, year + ZCTA5 ~ variable, value.var = "estimate")[!is.na(ZCTA5)] -> census_db_zcta_wide

  # Combine with addressStream by ZCTA or zip

  addressStream[!is.na(cxy_tract_id), GEOID := sprintf("%02d%03d%06d", as.integer(cxy_state_id), as.integer(cxy_county_id), as.integer(cxy_tract_id))]
  addressStream <- census_db_zcta_wide[
    census_db_wide[addressStream, , on = c("GEOID", "year"), roll = "nearest"], ,
    on = c("ZCTA5" = "zipcode", "year"), roll = "nearest"
  ]

  cols <- grep("^i\\.", colnames(addressStream), value = T) %>% sub(pattern = "^i\\.", replacement = "")
  for (col in cols) {
    addressStream[!is.na(get(paste0("i.", col))), (col) := get(paste0("i.", col))]
    addressStream[, (paste0("i.", col)) := NULL]
  }


  # Output ------------------------------------------------------------------

  # make distance and bearing features
  addressStream[, c("addressDistanceLevel", "addressBearingLevel") := mget(c("distance", "bearing"))][, c("distance", "bearing") := NULL]

  # convert demographic data to percentages
  demography <- grep("^address(?!Median|_|Total|Distance|Bearing|$)", colnames(addressStream), value = T, perl = T)
  medians <- grep("^addressMedian", colnames(addressStream), value = T)
  addressStream[addressTotalLevel > 0, (demography) := lapply(.SD, function(x) {
    x * 1.0 / get("addressTotalLevel")
  }), .SDcols = demography]

  # replace 0 medians and total with NA
  addressStream[, (c(medians, "addressTotalLevel")) := lapply(.SD, function(.) {
    if_else(. == 0, NA_real_, as.double(.))
  }),
  .SDcols = c(medians, "addressTotalLevel")
  ]

  # update values for customers who move
  setkey(addressStream, group_customer_no, timestamp)
  addressStream[, I := 1:.N, by = c("group_customer_no")]

  for (i in seq(2, addressStream[, max(I)])) {
    # essentially a rolling log average but taking into account NAs
    addressStream[I == i - 1 & group_customer_no == lead(group_customer_no) | I == i & group_customer_no == lag(group_customer_no),
                  (medians) := lapply(.SD, function(x) {
                    i0 <- log(x[seq(1, length(x), 2)] + .01)
                    i1 <- log(x[seq(2, length(x), 2)] + .01)
                    i1 <- coalesce(i1, i0)
                    i0 <- coalesce(i0, i1)
                    i1 <- exp((i1 + i0) / 2)
                    x[seq(2, .N, 2)] <- i1
                    x
                  }),
                  .SDcols = medians
    ]
    # bayesian updating of percentages
    addressStream[I == i - 1 & group_customer_no == lead(group_customer_no) | I == i & group_customer_no == lag(group_customer_no),
                  (demography) := lapply(.SD, function(x) {
                    i0 <- x[seq(1, length(x), 2)]
                    i1 <- x[seq(2, length(x), 2)]
                    i1 <- coalesce(i1, i0)
                    i0 <- coalesce(i0, i1)
                    i1 <- (i1 + i0) / 2
                    # i1 = coalesce(i1*i0/(i1*i0+(1-i1)*(1-i0)),i0)
                    x[seq(2, .N, 2)] <- i1
                    x
                  }),
                  .SDcols = demography
    ]
  }

  addressStream[, I := NULL]

  # Add in iWave data

  i <- read_tessi("iwave") %>%
    collect() %>%
    setDT()
  i[, .(group_customer_no,
        event_type = "Address", event_subtype = "iWave Score",
        addressProScoreLevel = pro_score, addressCapacityLevel = capacity_value,
        addressPropertiesLevel = properties_total_value, addressDonationsLevel = donations_total_value, timestamp = floor_date(score_dt, "day")
  )] %>%
    rbind(addressStream, fill = T) -> addressStream

  # Done!

  addressStreamFull <- copy(addressStream)
  addressStream <- addressStream[primary_ind == "y" & group_customer_no >= 200]

  # drop cxy columns
  addressStream[, c(
    "cluster", "vintageName", "GEOID", "ZCTA5", "year", "I", "primary_ind",
    grep("^(cxy|libpostal|google)", colnames(addressStream), value = T)
  ) := NULL]
  addressStream[, timestamp := timestamp_day][, timestamp_day := NULL]

  addressStream <- addressStream %>%
    mutate_all(adaptVmode) %>%
    as.ffdf()
  addressStreamFull <- addressStreamFull %>%
    mutate_all(adaptVmode) %>%
    as.ffdf()

  pack.ffdf(file.path(streamDir, "addressStream.gz"), addressStream)
  pack.ffdf(file.path(streamDir, "addressStreamFull.gz"), addressStreamFull)
  rm(list = ls())
  save.image()
}

if (FALSE) {








  # Add geocode info from the zipcodeR database

  zip_code_db <- as.data.table(zip_code_db)
  BAM_center <- c(-73.977765, 40.686876)

  # But not every postal code has a centroid so we need to do some fancy re-matching
  # First clean the postal codes so they are all five digits and make a 2-digit blocking criteria
  addressStream[
    !is.na(postal_code) & country == "usa" & stringr::str_length(postal_code) > 4 &
      grepl("^\\d+$", postal_code) & as.numeric(postal_code) > 0,
    .(
      zipcode = substr(postal_code, 1, 5),
      zipcode2 = substr(postal_code, 1, 2)
    )
  ] %>%
    distinct() %>%
    merge(zip_code_db[!is.na(lat) & !is.na(lng), .(zipcode, lat, lng,
                                                   zipcode2 = substr(zipcode, 1, 2)
    )],
    by = "zipcode2", all.x = T, allow.cartesian = T
    ) %>%
    .[!is.na(lat) & !is.na(lng)] %>%
    # Then join them together using the closest (numerically) postal code within the blocking criteria
    .[, diff := abs(as.numeric(zipcode.x) - as.numeric(zipcode.y))] %>%
    setkey(zipcode.x, diff) %>%
    .[, .SD[1], by = "zipcode.x"] %>%
    .[, .(zipcode = zipcode.x, lat, lng)] %>%
    # Then merge it all together
    merge(addressStream[, zipcode := substr(postal_code, 1, 5)], all.y = T, by = "zipcode") -> addressStream

  # 99.9% of valid postal codes now have a geocode
  testit::assert(addressStream[stringr::str_length(postal_code) > 4 & grepl("^\\d+$", postal_code) & country == "usa"] %>%
                   {
                     nrow(.[is.na(lat)]) / nrow(.)
                   } < .001)

  # remove blanks
  addressStream[, colnames(addressStream) := lapply(.SD, function(c) {
    if (is.character(c)) {
      replace(c, which(c == ""), NA)
    } else {
      c
    }
  })]
}
