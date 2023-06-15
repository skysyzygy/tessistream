# Geocoding ---------------------------------------------------------------

#' address_geocode
#'
#' Geocodes addresses using the [tidygeocoder] package.
#' Tries each address up to six times, using `libpostal` parsing, `street1`, and `street2`, and the
#' US census and openstreetmap geocoders.
#'
#' @param address_stream data.table of addresses
#'
#' @return data.table of addresses, one row per address in input, must include `address_cols`. Contains only address_cols and columns returned by `tidygeocoder`
#' @importFrom tidygeocoder geocode_combine
#' @importFrom purrr map flatten map_chr
#' @importFrom tidyr expand_grid
#' @importFrom checkmate assert_data_table assert_names
#' @describeIn address_geocode geocode all addresses
address_geocode_all <- function(address_stream) {
  . <- libpostal.street <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  # Add index columns
  address_stream_parsed <- address_stream[,I:=.I]
  address_stream <- address_stream[,..address_cols] %>% .[,I:=.I]

  # build libpostal street name
  if(any(c("libpostal.house_number","libpostal.road") %in% colnames(address_stream_parsed))) {
     address_stream_parsed <- address_stream_parsed %>%
      setunite("libpostal.street", any_of(c("libpostal.house_number", "libpostal.road")), sep = " ", na.rm = TRUE)
    address_stream_parsed[libpostal.street == "",libpostal.street := NA_character_]
    address_cols <- c("libpostal.street", address_cols)
  }

  # suppress completely missing data
  address_stream_parsed <-
    address_stream_parsed[!address_stream_parsed[,lapply(.SD,is.na),.SDcols = address_cols] %>% purrr::reduce(`&`)]

  if(nrow(address_stream_parsed) == 0)
    return(address_stream[,setdiff(address_cols, "libpostal.street")])

  # fill NAs with blanks because these aren't handled well in some parsers
  setnafill(address_stream_parsed,"const",fill="",address_cols)
  address_stream_parsed[,(address_cols) := lapply(.SD,as.character), .SDcols=address_cols]

  global_params <- list(city = "city", state="state", postalcode = "postal_code",
                    return_input = TRUE, full_results = TRUE)

  # Make the list of queries to run
  queries <- expand_grid(params = list(list(method = 'census',
                                            mode = "batch",
                                            api_options = list(census_return_type = "geographies")),
                                       list(method = 'osm',
                                            country = "country")),
                         street = as.character(grep("street",address_cols,value=T))) %>%
    split(1:nrow(.)) %>%
    map(tidyr::unnest_wider,"params") %>%
    map(flatten)

  # RUn the queries
  result <- geocode_combine(address_stream_parsed,
                    queries = queries,
                    global_params = global_params,
                    lat = "lat", long = "lon",
                    query_names = map_chr(queries,~paste(.$method,.$street))) %>% setDT

  # Throw away list columns
  result <- purrr::keep(result, is.atomic)

  result[address_stream,
         setdiff(colnames(result),colnames(address_stream_parsed)),
         with = F,
         on = "I"] %>% cbind(address_stream) %>% .[,I:=NULL]

}

#' @describeIn address_geocode geocode only uncached addresses, load others from cache
address_geocode <- function(address_stream) {
  address_cache_parallel(address_stream, "address_geocode", address_geocode_all)
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
  . <- census_tract <- lat <- lon <- i.state_fips <- i.county_fips <- i.census_tract <- i.census_block <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  # filter and add index columns
  address_stream <- address_stream[,..address_cols] %>% .[,I:=.I]
  address_stream_geocode <- address_geocode(address_stream) %>%
    .[,c("state_fips", "county_fips", "census_tract", "census_block", "lat", "lon"), with = F] %>%
    .[,I:=.I]

  # don't reverse geocode things that are already done or can't be reversed
  to_reverse <- address_stream_geocode[is.na(census_tract) & !is.na(lat) & !is.na(lon)] %>% unique

  if(nrow(to_reverse) > 0) {

    usa <- nation()
    points <- sf::st_as_sf(to_reverse[,.(lon = as.numeric(lon),
                                         lat = as.numeric(lat))],
                           coords=c("lon","lat"), crs="WGS84") %>%
      sf::st_transform(sf::st_crs(usa))

    # don't reverse things not in the US
    to_reverse <- to_reverse[as.logical(sf::st_contains(usa, points, sparse = F)), ] %>%
      address_cache_parallel("address_reverse_census", address_reverse_census_all, key_cols = c("lat","lon"))
  }

  address_stream_geocode[to_reverse,
                         `:=`(state_fips=i.state_fips,
                              county_fips=i.county_fips,
                              census_tract=i.census_tract,
                              census_block=i.census_block),
                         on=c("lat","lon")]

  address_stream_geocode[address_stream,on = "I"] %>% .[,I:=NULL] %>% .[]

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
address_reverse_census_all <- function(address_stream) {
  . <- lat <- lon <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = c("lat","lon"))
  address_stream <- address_stream[,.(lat,lon)]

  address_reverse <- Vectorize(cxy_geography, SIMPLIFY = FALSE)(lon = address_stream$lon,
                                                                lat = address_stream$lat) %>%
    purrr::modify_if(is.null,~list(NA)) %>% rbindlist(fill=T)

  columns <- Vectorize(grep, "pattern")(paste0("Census\\.Blocks\\.",c("STATE","COUNTY","TRACT","BLOCK")),
                                        colnames(address_reverse), value = T)

  address_reverse[,..columns] %>% setnames(c("state_fips","county_fips","census_tract","census_block")) %>%
    cbind(address_stream)
}

