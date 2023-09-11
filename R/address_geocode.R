# Geocoding ---------------------------------------------------------------

#' address_geocode
#'
#' Geocodes addresses using the [tidygeocoder] package.
#' Tries each address up to six times, using `libpostal` parsing (if `libpostal.house_number` and `libpostal.road` are passed in as columnes),
#' `street1`, and `street2`, and the US census and openstreetmap geocoders.
#'
#' @param address_stream data.table of addresses
#' @param num_tries integer number of times to retry on error
#'
#' @return data.table of addresses, one row per address in input, must include `address_cols`. Contains only address_cols and columns returned by `tidygeocoder`
#' @importFrom tidygeocoder geocode_combine
#' @importFrom purrr map flatten map_chr
#' @importFrom tidyr expand_grid any_of all_of
#' @importFrom checkmate assert_data_table assert_names
#' @describeIn address_geocode geocode all addresses
address_geocode_all <- function(address_stream, num_tries = 10) {
  . <- libpostal.street <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  # Add index columns
  address_stream_parsed <- address_stream[,I:=.I]
  address_stream <- address_stream[,c(address_cols,"I"),with=F]

  # build libpostal street name
  if(any(c("libpostal.house_number","libpostal.road") %in% colnames(address_stream_parsed))) {
     address_stream_parsed <- address_stream_parsed %>%
      setunite("libpostal.street", any_of(c("libpostal.house_number", "libpostal.road", "libpostal.house")), sep = " ", na.rm = TRUE)
    address_stream_parsed[libpostal.street == "",libpostal.street := NA_character_]
    address_cols <- c("libpostal.street", address_cols)
  }

  # blanks to NAs
  for (address_col in address_cols)
    address_stream_parsed[trimws(get(address_col)) == "",(address_col) := NA]

  # suppress completely missing data
  address_stream_parsed <-
    address_stream_parsed[!address_stream_parsed[,lapply(.SD,is.na),.SDcols = address_cols] %>% purrr::reduce(`&`)]
  if(nrow(address_stream_parsed) == 0)
    return(address_stream[,setdiff(address_cols, "libpostal.street"), with = F])

  # make address fields for queries
  street_cols <- as.character(grep("street",address_cols,value=T))
  address_street_cols <- paste0("address_",street_cols)
  for (street_col in street_cols)
    setunite(address_stream_parsed,paste0("address_",street_col),
             all_of(c(street_col,"city","state","postal_code","country")), sep = ", ", na.rm = TRUE, remove = FALSE)

  # remove duplicates
  for (i in rev(seq_along(address_street_cols)[-1]))
    address_stream_parsed[get(address_street_cols[i]) %in% mget(address_street_cols[-i]), (address_street_cols[i]) := NA ]

  global_params <- list(full_results = TRUE)

  # Make the list of queries to run
  queries <- expand_grid(params = list(list(method = "census",
                                            mode = "batch",
                                            api_options = list(census_return_type = "geographies")),
                                       list(method = "google"),
                                       list(method = "osm",
                                            min_time = 1.5)),
                         address = address_street_cols) %>%
    split(1:nrow(.)) %>%
    map(tidyr::unnest_wider,"params") %>%
    map(flatten)

  # Run the queries
  result <- make_resilient(geocode_combine(address_stream_parsed,
                    queries = queries,
                    global_params = global_params,
                    return_list = TRUE,
                    lat = "lat", long = "lon",
                    query_names = map_chr(queries,~paste(.$method,.$address))),
                    default = address_stream)

  if(!is.data.frame(result))
     result <- rbindlist(result, fill = TRUE, idcol = "query")

  # Throw away list columns
  result <- purrr::keep(result, is.atomic)

  result[address_stream,
         setdiff(colnames(result),colnames(address_stream_parsed)),
         with = F,
         on = "I"] %>% cbind(address_stream) %>%
    .[,I:=NULL]

}

#' @describeIn address_geocode geocode only uncached addresses, load others from cache
address_geocode <- function(address_stream) {
  address_cache_chunked(address_stream, "address_geocode", address_geocode_all, n = 1000, parallel = F)
}

#' address_reverse_census
#'
#' Gets census geography (tract/block/county/state) information for US addresses. Skips
#' lat/lon pairs that aren't in the US and which have already been forward geocoded and caches results.
#'
#' @param address_stream data.table of addresses, must include `address_cols`
#'
#' @return data.table of addresses, one row per address in `address_stream`.  Contains only address_cols and
#' "state_fips", "county_fips", "census_tract", "census_block", "lat", "lon"
#' @importFrom tigris nation
address_reverse_census <- function(address_stream) {
  . <- census_tract <- lat <- lon <- state_fips <- county_fips <- census_tract <- census_block <-
    i.state_fips <- i.county_fips <- i.census_tract <- i.census_block <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)


  # gecode if needed and add index columns
  address_stream[,I:=.I]
  address_stream_geocode <- if(!all(c("state_fips", "county_fips", "census_tract", "census_block", "lat", "lon") %in% colnames(address_stream))) {
    address_geocode(address_stream) %>% .[,.(state_fips, county_fips, census_tract, census_block, lat, lon, .I)]
  } else {
    address_stream
  }

  # don't reverse geocode things that are already done or can't be reversed
  to_reverse <- address_stream_geocode[is.na(census_tract) & !is.na(lat) & !is.na(lon), .(lat,lon)] %>% unique

  if(nrow(to_reverse) > 0) {

    usa <- nation()
    points <- sf::st_as_sf(to_reverse[,.(lon = as.numeric(lon),
                                         lat = as.numeric(lat))],
                           coords=c("lon","lat"), crs="WGS84") %>%
      sf::st_transform(sf::st_crs(usa))

    # don't reverse things not in the US
    to_reverse <- to_reverse[as.logical(sf::st_contains(usa, points, sparse = F)), ] %>%
      address_cache_chunked("address_reverse_census", address_reverse_census_all, key_cols = c("lat","lon"))
  }

  address_stream_geocode[to_reverse,
                         `:=`(state_fips=i.state_fips,
                              county_fips=i.county_fips,
                              census_tract=i.census_tract,
                              census_block=i.census_block),
                         on=c("lat","lon")]

  address_stream_geocode[address_stream, on = "I"]

}

#' address_reverse_census_all
#'
#' Gets census geography (tract/block/county/state) information for US addresses using [censusxy::cxy_geography].
#' Calls [cxy_geography] once per row.
#'
#' @param address_stream data.table of addresses, must include `lat` and `lon`
#'
#' @return data.table of census geographies, one row per lat/lon pair in `address_stream`
#' @importFrom censusxy cxy_geography
#' @importFrom data.table rbindlist setnames
#' @importFrom purrr modify_if
address_reverse_census_all <- function(address_stream) {
  . <- lat <- lon <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = c("lat","lon"))
  address_stream <- address_stream[,.(lat,lon)]

  address_reverse <- apply(address_stream, 1, \(.) try(cxy_geography(lon = .["lon"], lat = .["lat"]))) %>%
    modify_if(~!is.list(.),~list(NA)) %>% rbindlist(fill = TRUE)

  columns <- Vectorize(grep, "pattern")(paste0("Census\\.Blocks\\.",c("STATE","COUNTY","TRACT","BLOCK"),"$"),
                                        colnames(address_reverse), value = T)

  address_reverse[,columns, with = FALSE] %>%
    setnames(c("state_fips","county_fips","census_tract","census_block")) %>%
    cbind(address_stream)
}

