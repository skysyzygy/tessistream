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
  label <- concept <- name <- type <- sex <- age <- dataset <- race <- measure <- NULL

  datasets <- expand_grid(year = seq(2000,year(Sys.Date())),
                          dataset = c("acs5/profile","sf1","sf3")) %>% setDT

  variables <- .mapply(census_variables_load, datasets, list()) %>% rbindlist

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

census_variables_load <- function(year, dataset) {
  . <- NULL

  tryCatch(load_variables(year, dataset, cache = TRUE) %>% setDT %>% .[,`:=`(year = year,
                                                                             dataset = dataset)],
           error = \(e) data.table())


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
#' @importFrom purrr map
census_get_data <- function(year,dataset,variables) {
  . <- NULL

  assert_integerish(year,len = 1)
  assert_character(dataset, len = 1)
  assert_character(variables, min.len = 1, any.missing = FALSE)

  census_getter <- if (dataset %in% c("sf1","sf3")) {
    get_decennial
  } else {
    get_acs
  }

  census_data <- map(tidycensus::fips_codes[,"state_code"] %>% unique,
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

  address_cache_chunked(census_variables, "address_census", census_get_data_all, n = 10,
                key_cols = c("year","dataset","variable"),
                ...) %>%
    merge(census_variables, by=c("year","dataset","variable"))

}

#' @describeIn census_data Loads census data (does not cache)
#' @importFrom data.table uniqueN
census_get_data_all <- function(census_variables) {
  assert_data_table(census_variables)
  assert_names(colnames(census_variables), must.include = c("year","dataset","variable"))

  split(census_variables,by=c("year","dataset")) %>%
    purrr::map(~census_get_data(.$year[[1]],.$dataset[[1]],.$variable)) %>%
    rbindlist(fill = TRUE)

}

#' @describeIn census_features Prepares census race/ethnicity data
census_race_features <- function() {
  . <- value <- estimate <- moe <- GEOID <- type <- dataset <- race <- feature <- NULL
  race_variables <- census_variables()[type == "race" & (year != 2010 | dataset == "sf1")]
  race_features <- census_data(race_variables) %>% .[,.(year, GEOID, value = dplyr::coalesce(value,estimate), moe, feature = race)] %>%
    .[,feature := ifelse(grepl("some other",feature,ignore.case = TRUE),"Other",feature)]

}

#' @describeIn census_features Prepares census sex data
census_sex_features <- function() {
  . <- value <- estimate <- moe <- GEOID <- type <- dataset <- sex <- age <- NULL
  sex_variables <- census_variables()[type == "sex_and_age" & sex %in% c("Female","Male") & is.na(age) & (year != 2010 | dataset == "sf1")]
  sex_features <- census_data(sex_variables)[,value := coalesce(value,estimate)] %>%
    .[,.(value = sum(value,na.rm=T),moe = sum(moe,na.rm = TRUE)), by = list(year,GEOID,feature = sex)]

}

#' @describeIn census_features Prepares census age data
census_age_features <- function() {
  . <- value <- estimate <- moe <- GEOID <- type <- dataset <- age <- NULL
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
  . <- value <- estimate <- moe <- GEOID <- type <- dataset <- measure <- NULL
  income_variables <- census_variables()[type == "income" & !is.na(measure)]
  income_features <- census_data(income_variables) %>% .[,.(year,GEOID,value = dplyr::coalesce(value,estimate),moe,feature = paste(measure,"Income"))]

}

#' census_features
#'
#' Prepare features from raw census data
#'
#' @return data.table of features extracted from census datasets
census_features <- function() {
  . <- feature <- NULL

  rbind(census_race_features(),
        census_sex_features(),
        census_age_features(),
        census_income_features()) %>%
    # snake_case them
    .[,feature := paste("address",tolower(feature),"level") %>% gsub("\\s+","_", .), by = "feature"] %>%
    data.table::dcast(year + GEOID ~ feature, fun.aggregate = sum, na.rm = TRUE, value.var = "value")

}

#' address_census
#'
#' Append census features to a data.table of addresses based on matching `address_cols` and `timestamp`
#'
#' @param address_stream data.table of addresses
#' @importFrom lubridate year
#' @return data.table of addresses, one row per input row. Contains `address_cols`, `timestamp` and added census features.
address_census <- function(address_stream) {
  . <- timestamp <- state_fips <- county_fips <- census_tract <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = c(address_cols,"timestamp"))

  address_stream <- cbind(address_reverse_census(address_stream[,..address_cols,with=F]),
                          timestamp = address_stream$timestamp) %>%
    .[,`:=`(GEOID = paste0(state_fips,county_fips,census_tract),
            year = year(address_stream$timestamp))]

  census_features <- census_features()
  address_stream <- census_features[address_stream,c(address_cols,"timestamp",colnames(census_features)),on=c("GEOID","year"),roll="nearest",with=F] %>%
    .[,`:=`(GEOID = NULL, year = NULL)]

  # convert demographic data to percentages
  demography_cols <- grep("^address_(?!median|mean)", colnames(address_stream), value = T, perl = T)
  address_stream[, sum := sum(.SD, na.rm = T), .SDcols = demography_cols, by = 1:nrow(address_stream)]
  address_stream[, (demography_cols) := purrr::map(.SD, ~{ . * 1.0 / sum * 3 }), .SDcols = demography_cols]
  address_stream[, sum := NULL]

  # # make distance and bearing features
  # address_stream[, c("address_distance_level", "address_bearing_level") := mget(c("distance", "bearing"))][, c("distance", "bearing") := NULL]

  # replace 0 income with NA
  income_cols <- grep("^address_(median|mean)", colnames(address_stream), value = T)
  purrr::walk(income_cols, ~address_stream[get(.x) == 0, c(.x) := NA_real_])

  address_stream
}



