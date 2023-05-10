
# Geocoding ---------------------------------------------------------------

#' address_geocode_all
#'
#' Geocodes addresses using the [tidygeocoder] package . Tries each address up to six times, using `libpostal` parsing, `street1`, and `street2`, and the
#' US census and openstreetmap geocoders.
#'
#' @param address_stream data.table of addresses, must include `address_cols`
#'
#' @return data.table of addresses, one row address in `address_stream`
#' @importFrom tidygeocoder geocode_combine
#' @importFrom purrr cross2 map flatten map_chr
#' @importFrom checkmate assert_data_table assert_names
address_geocode_all <- function(address_stream) {
  libpostal.street <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  address_stream_parsed <- address_parse(address_stream)

  # build libpostal street name
  if(any(c("libpostal.house_number","libpostal.road") %in% colnames(address_stream_parsed))) {
     address_stream_parsed <- address_stream_parsed %>%
      unite("libpostal.street", any_of(c("libpostal.house_number", "libpostal.road")), sep = " ", na.rm = TRUE)
    address_stream_parsed[libpostal.street == "",libpostal.street := NA_character_]
    address_cols <- c(address_cols, "libpostal.street")
  }

  # suppress completely missing data
  address_stream_parsed <-
    address_stream_parsed[!address_stream_parsed[,lapply(.SD,is.na),.SDcols = address_cols] %>% purrr::reduce(`&`)]

  if(nrow(address_stream_parsed) == 0)
    return(address_stream_parsed)

  # fill NAs with blanks because these aren't handled well in some parsers
  setnafill(address_stream_parsed,"const",fill="",address_cols)
  address_stream_parsed[,(address_cols) := lapply(.SD,as.character), .SDcols=address_cols]

  global_params <- list(city = "city", state="state", postalcode = "postal_code",
                  return_input = TRUE, full_results = TRUE, progress_bar = TRUE)

  queries <- cross2(map(sort(grep("street",as.character(address_cols),value=T)),~list(street=.)),
                    list(list(method = 'census',
                              api_options = list(census_return_type = "geographies")),
                         list(method = 'osm',
                              country = "country"))) %>% map(flatten)

  geocode_combine(address_stream_parsed,
                  queries = queries,
                  global_params = global_params,
                  query_names = map_chr(queries,~paste(.$method,.$street)),
                  lat = "lat",
                  long = "lng") %>% setDT

}

address_geocode <- function(address_stream) {
  address_cache(address_stream, "address_geocode", address_geocode_all)
}

#' address_census_geography
#'
#' Gets census geography (tract/block/county/state) information for US addresses.
#'
#' @param address_stream data.table of addresses, must include `address_cols`
#'
#' @return data.table of addresses, one row address in `address_stream`
#'
address_census_geography <- function(address_stream) {
  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  address_stream_geocode <- address_geocode(address_stream)

  address_reverse <- address_stream_geocode[is.na(census.tract_id) & !is.na(lat) & !is.na(lng) & inside(usa_bbox)] %>% distinct

  address_cache(address_reverse, "address_geocode", address_reverse_census_geography, update = TRUE)

}

address_reverse_census_geography <- function(address_stream) {
  address_reverse <- cxy_geography(lng, lat)

}


if(FALSE) {

  # Reverse geocoding -------------------------------------------------------

  # Add geoid data (tract/block/county/state) to rows that are missing it
  addressStream[
    is.na(cxy_tract_id) & !is.na(lat) & !is.na(lng) & (country == "usa" | google.country == "usa"),
    .(cxy_lon = lng, cxy_lat = lat, benchmark, vintageName)
  ] %>% distinct() %>%
    # eliminate anything we've already done
    .[!geocode_db, , on = c("cxy_lon", "cxy_lat", "vintageName")] %>%
    # and then associate a cluster if there are a lot to do
    .[, cluster := rep(seq(.N), length.out = .N, each = 100)] -> for_geocoding

  with_progress({
    p <- progressor(n_distinct(for_geocoding$cluster))
    for_geocoding %>%
      group_split(cluster) %>%
      lapply(function(.x) {
        future({
          ret <- lapply(transpose(.x), function(.) {
            cxy_geography(.[1], .[2], .[3], vintage = coalesce(.[4], "Current_Current"))
          }) %>% rbindlist(., fill = T)
          p()
          ret
        })
      }) %>%
      value() -> geocode_temp
  })

  geocode_temp %>%
    rbindlist(fill = T) %>%
    .[, .(
      cxy_state_id = States.STATE,
      cxy_county_id = Counties.COUNTY,
      cxy_tract_id = Census.Tracts.TRACT,
      cxy_block_id = Census.Blocks.BLOCK
    )] %>%
    cbind(for_geocoding[, -c("benchmark")]) %>%
    rbind(geocode_db, fill = T) -> geocode_db
  save(geocode_db, file = "geocode_db.RData")

  addressStream[
    geocode_db[addressStream[
      is.na(cxy_tract_id) & !is.na(lat) & !is.na(lng) & (country == "usa" | google.country == "usa"),
      .(cxy_lon = lng, cxy_lat = lat, benchmark, vintageName, I)
    ],
    .(cxy_state_id, cxy_county_id, cxy_tract_id, cxy_block_id, I),
    on = c("cxy_lon", "cxy_lat", "vintageName")
    ],
    `:=`(
      cxy_state_id = i.cxy_state_id,
      cxy_county_id = i.cxy_county_id,
      cxy_tract_id = i.cxy_tract_id,
      cxy_block_id = i.cxy_block_id
    ),
    on = "I"
  ] -> addressStream

  # 99.9% of geocoded US addresses have census tract info
  testit::assert(addressStream[!is.na(lat) & !is.na(lng) & (country == "usa" | google.country == "usa")] %>%
    {
      .[is.na(cxy_tract_id), .N] / .[, .N]
    } < .001)

  # Add distance and bearing info to addressStream
  with_progress({
    p <- progressor(100)
    addressStream[!is.na(lat) & !is.na(lng), .(lat, lng, I, cluster = rep(seq(100), length.out = .N))] %>%
      group_split(cluster) %>%
      lapply(function(.) {
        future(
          {
            ret <- setDT(.)[, `:=`(
              distance = distm(cbind(lng, lat), BAM_center, distCosine),
              bearing = bearingRhumb(BAM_center, cbind(lng, lat))
            )]
            p()
            ret
          },
          packages = c("geosphere")
        )
      })
  }) %>%
    value() %>%
    rbindlist() %>%
    .[addressStream, c(colnames(addressStream), c("distance", "bearing")), on = "I", with = F] -> addressStream


  # Census data -------------------------------------------------------------

  # census_api_key("6d0fd688fc79ce5d0f5484785dfc4b3717801436",install=T,overwrite=T)

  # Get variable names for ACS 5-year surveys
  variables <- lapply(seq(2009, 2019), function(year) {
    load_variables(year, "acs5/profile", cache = T) %>%
      mutate(year = year) %>%
      {
        rbind(
          filter(., grepl("^(number|estimate).+sex and age.+(to |85)", label, ignore.case = T)) %>% mutate(type = "sex_and_age"),
          filter(., grepl("^(number|estimate).+one race!![^!]+$", label, ignore.case = T)) %>% mutate(type = "race"),
          filter(., grepl("^(number|estimate).+(median|mean) household income", label, ignore.case = T)) %>% mutate(type = "income")
        )
      }
  }) %>%
    rbindlist() %>%
    # and the 2000 decennial survey
    rbind(rbind(
      load_variables(2000, "sf1", cache = T) %>% mutate(sumfile = "sf1", year = 2000),
      load_variables(2000, "sf3", cache = T) %>% mutate(sumfile = "sf3", year = 2000)
    ) %>%
      {
        rbind(
          filter(., grepl("Median age!!Both sexes", label, ignore.case = T) & grepl("^MEDIAN.+X \\[", concept)) %>% mutate(type = "sex_and_age"),
          filter(., grepl("one.+race!![^!]+$", label, ignore.case = T) & grepl("^RACE \\[", concept, perl = T)) %>% mutate(type = "race"),
          filter(., grepl("(median|mean) household income", label, ignore.case = T) & grepl("DOLLARS) \\[", concept)) %>% mutate(type = "income")
        )
      }, fill = T) %>%
    mutate(label = stringr::str_extract(label, "[^!]+$")) %>%
    select(-concept)

  if (file.exists("census_db.RData")) {
    load("census_db.RData")
  }

  if (!exists("census_db")) {
    data(state)
    with_progress({
      p <- progressor(10)
      # tract-level data begins with the 2010 ACS
      lapply(c(2000, seq(2011, 2019)), function(year) {
        future(
          {
            if (year == 2000) {
              ret <- rbind(
                get_decennial(
                  geography = "tract",
                  state = state.abb,
                  year = 2000,
                  sumfile = "sf1",
                  cache_table = T,
                  variables = variables[year == 2000 & sumfile == "sf1", name],
                  show_call = T
                ),
                get_decennial(
                  geography = "tract",
                  state = state.abb,
                  year = 2000,
                  sumfile = "sf3",
                  cache_table = T,
                  variables = variables[year == 2000 & sumfile == "sf3", name],
                  show_call = T
                )
              )
            } else {
              ret <- get_acs(
                geography = "tract",
                state = state.abb,
                year = year,
                cache_table = T,
                variables = variables[year == year, name],
                show_call = T
              )
            }
            p()
            mutate(ret, year = year)
          },
          packages = c("data.table")
        )
      })
    }) %>%
      value() %>%
      rbindlist(fill = T) -> census_db

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
