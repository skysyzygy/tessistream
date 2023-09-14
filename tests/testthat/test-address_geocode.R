withr::local_package("checkmate")
withr::local_package("mockery")

tessilake:::local_cache_dirs()

do_geocoding <- T

# address_geocode_all --------------------------------------------------
test_that("address_geocode_all uses libpostal and non-libpostal data", {

  address_stream <- data.table(
    street1 = "30 street1",
    street2 = "30 street2",
    city = "Brooklyn",
    state = "NY",
    country = "USA",
    postal_code = "11217"
  )

  geocode_combine <- mock(cbind(address_stream, I = 1, lat = 123, lon = 456), cycle = TRUE)
  stub(address_geocode_all,"geocode_combine",geocode_combine)

  address_geocode_all(address_stream)
  expect_equal(mock_args(geocode_combine)[[1]][[1]]$street1,"30 street1")
  expect_false("libpostal.street" %in% colnames(mock_args(geocode_combine)[[1]][[1]]))

  address_stream <- cbind(address_stream,libpostal.house_number = "30", libpostal.road = "libpostal.road")

  address_geocode_all(address_stream)
  expect_equal(mock_args(geocode_combine)[[2]][[1]]$libpostal.street,"30 libpostal.road")

})

test_that("address_geocode_all runs census + bing + osm x 3 queries using tidygeocoder", {

  address_stream <- data.table(
    street1 = "30 street1",
    street2 = "30 street2",
    city = "Brooklyn",
    state = "NY",
    country = "USA",
    postal_code = "11217",
    libpostal.house_number = "30",
    libpostal.road = "libpostal.road"
  )

  geocode_combine <- function(global_params,...) { tidygeocoder::geocode_combine(global_params = c(global_params, no_query = TRUE), ...)}
  stub(address_geocode_all,"geocode_combine",geocode_combine)

  msg <- capture.output(capture.output(address_geocode_all(address_stream),type="message"))
  expect_match(paste(msg,collapse="\\n"),paste0("Census.+batch.+",
                                                "Census.+batch.+",
                                                "Census.+batch.+",
                                                "Google.+libpostal.+",
                                                "Google.+street1.+",
                                                "Google.+street2.+",
                                                "openstreetmap.+libpostal.+",
                                                "openstreetmap.+street1.+",
                                                "openstreetmap.+street2.+"))

})

test_that("address_geocode_all doesn't retry when street info is duplicated", {
  address_stream <- data.table(
    street1 = "30 street",
    street2 = "30 street",
    city = "Brooklyn",
    state = "NY",
    country = "USA",
    postal_code = "11217",
    libpostal.house_number = "30",
    libpostal.road = "street"
  )

  geocode_combine <- function(global_params,...) { tidygeocoder::geocode_combine(global_params = c(global_params, no_query = TRUE), ...)}
  stub(address_geocode_all,"geocode_combine",geocode_combine)

  msg <- capture.output(capture.output(address_geocode_all(address_stream),type="message"))

  expect_match(paste(msg,collapse="\\n"),paste0("Census batch.+",
                                                "Returning NA results.+",
                                                "Returning NA results.+",
                                                "Google.+30 street.+",
                                                "Returning NA results.+",
                                                "Returning NA results.+",
                                                "openstreetmap.+30 street.+",
                                                "Returning NA results.+",
                                                "Returning NA results.+"))
})


if(do_geocoding) {
  test_that("address_geocode_all gets US and international addresses", {

    address_stream <- data.table(
      libpostal.house_number = "30",
      libpostal.road = c("Lafayette Ave","Churchill Pl"),
      street1 = c("30 Lafayette Ave","30 Churchill Pl"),
      street2 = c("30 Lafayette Ave","30 Churchill Pl"),
      city = c("Brooklyn","London"),
      state = c("NY",NA),
      country = c("USA","UK"),
      postal_code = c("11217","E14 5EU")
    )

    capture.output(res <- address_geocode_all(address_stream), type = "message")

    # All get geocode
    expect_false(any(is.na(res[,.(lat,lon)])))
    # And those geocodes are for the right addresses
    expect_match(res[1,matched_address],"30 LAFAYETTE AVE")
    column_name <- intersect(c("formatted_address","display_name"),colnames(res))
    expect_match(res[2,..column_name][[1]],"30.+Churchill Pl")
    # And we get census tract data when available
    expect_false(is.na(res[1,census_tract]))

  })

  test_that("address_geocode_all works with missing data", {

    address_stream <- rbind(
     data.table(street1 = "30 Lafayette Ave"),
     data.table(street2 = "321 Ashland Pl"),
     data.table(city = "Brooklyn"),
     data.table(state = "NY"),
     data.table(country = "USA"),
     data.table(postal_code = "11217"),
     data.table(),
     fill = TRUE
    )

    n <- 0
    while(!exists("res") || any(is.na(res[1:6,.(lat,lon)])) && n < 10) {
      capture.output(res <- address_geocode_all(address_stream), type = "message")
      Sys.sleep(10)
      n <- n + 1
    }

    # All get geocode
    expect_false(any(is.na(res[1:6,.(lat,lon)])))
    # And those geocodes are for the right addresses
    column_name <- intersect(c("formatted_address","display_name"),colnames(res))
    expect_match(res[1,..column_name][[1]],"30.+Lafayette Ave")
    expect_match(res[2,..column_name][[1]],"321.+Ashland Pl")
    expect_match(res[3,..column_name][[1]],"Brooklyn")
    expect_match(res[4,..column_name][[1]],"New York")
    expect_match(res[5,..column_name][[1]],"United States")
    expect_match(res[6,..column_name][[1]],"11217")
    expect_equal(res[7,..column_name][[1]],NA_character_)
  })
}

test_that("address_geocode_all suppresses completely missing data because geocode_combine fails", {
  stub(address_geocode_all,"geocode_combine",function(.tbl,...) { message(nrow(.tbl)); cbind(.tbl,lat=123,lon=456)})

  address_stream <- data.table(
    libpostal.house_number = NA,
    libpostal.road = NA,
    street1 = NA,
    street2 = NA,
    city = NA,
    state = NA,
    country = NA,
    postal_code = NA
  )

  expect_silent(address_geocode_all(address_stream))

  address_stream <- rbind(address_stream,data.table(
    street1 = c("30 Lafayette Ave"),
    city = c("Brooklyn"),
    state = c("NY"),
    country = c("USA"),
    postal_code = c("11217")
  ), fill = T)

  expect_message(res <- address_geocode_all(address_stream), "1")

  expect_equal(nrow(res),2)

})

test_that("address_geocode_all returns one row per incoming address", {
  stub(address_geocode_all,"geocode_combine",function(.tbl,...) { cbind(.tbl,lat=123,lon=456)})

  address_stream <- rbind(
    data.table(street1 = "30 Lafayette Ave"),
    data.table(street2 = "321 Ashland Pl"),
    data.table(city = "Brooklyn"),
    data.table(state = "NY"),
    data.table(country = "USA"),
    data.table(postal_code = "11217"),
    data.table(),
    fill = TRUE
  )

  result <- address_geocode_all(address_stream)

  expect_equal(nrow(result), nrow(address_stream))
  expect_equal(result[,..address_cols],address_stream[,..address_cols])
  expect_equal(result$lat,rep(123,nrow(address_stream)))
  expect_equal(result$lon,rep(456,nrow(address_stream)))
  expect_named(result, c(address_cols,"lat","lon"), ignore.order = TRUE)
})

test_that("address_geocode_all doesn't copy input data.table", {

  address_stream <- rbind(
    data.table(street1 = "30 Lafayette Ave"),
    data.table(street2 = "321 Ashland Pl"),
    data.table(city = "Brooklyn"),
    data.table(state = "NY"),
    data.table(country = "USA"),
    data.table(postal_code = "11217"),
    data.table(),
    fill = TRUE
  )

  geocode_combine <- function(.tbl,...) { cbind(.tbl,lat=123,lon=456)}
  stub(address_geocode_all,"geocode_combine",geocode_combine)
  stub(address_geocode,"address_geocode_all",address_geocode_all)

  tracemem(address_stream)
  expect_silent(address_geocode_all(address_stream))

})

test_that("address_geocode_all is failure resistant", {

  address_stream <- rbind(
    data.table(street1 = "30 Lafayette Ave"),
    data.table(street2 = "321 Ashland Pl"),
    data.table(city = "Brooklyn"),
    data.table(state = "NY"),
    data.table(country = "USA"),
    data.table(postal_code = "11217"),
    data.table(),
    fill = TRUE
  )

  stub(address_geocode_all, "geocode_combine", \(...) stop("Geocoding error!"))
  stub(address_geocode_all, "make_resilient", \(expr, num_tries, sleep_secs, ...) make_resilient(expr, num_tries = 1, sleep_secs = 0, ...))

  expect_message(res <- address_geocode_all(copy(address_stream)), "Geocoding error!")
  expect_mapequal(res, address_stream)

})

# address_geocode ---------------------------------------------------------

test_that("address_geocode does not retry successfully geocoded addresses", {

  address_stream <- expand.grid(
    street1 = c("30 Lafayette Ave", "651 Fulton St"),
    street2 = "Brooklyn Academy of Music",
    city = "Brooklyn",
    country = "USA",
    state = c("NY", "New York"),
    postal_code = "11217"
  ) %>%
    lapply(as.character) %>%
    setDT()

  geocode_combine <- function(.tbl,...) { message(nrow(.tbl)); cbind(.tbl,lat=123,lon=456)}
  stub(address_geocode_all,"geocode_combine",geocode_combine)
  stub(address_geocode,"address_geocode_all",address_geocode_all)

  expect_message(result <- address_geocode(address_stream),"4")
  expect_silent(result2 <- address_geocode(address_stream))
  expect_equal(result,result2)

})


# address_reverse_census_all ----------------------------------------------

test_that("address_reverse_census_all turns lat/lon pairs into census tract info",{
  # NY, CA, MO, Toronto
  coords <- data.table(lat = c(44.731171, 36.778259, 37.964252),
                       lon = c(-75.428833, -119.417931, -91.831833))

  res <- address_reverse_census_all(coords)

  expect_equal(res$state_fips, c("36", "06", "29"))
  expect_equal(res$county_fips, c("089", "019", "161"))
  expect_named(res, c("state_fips","county_fips","census_tract","census_block","lat","lon"),
               ignore.order = TRUE)
  expect_equal(nrow(coords),nrow(res))

})

test_that("address_reverse_census_all gracefully works when non-US data is submitted",{
  # NY, CA, MO, Toronto
  coords <- data.table(lat = c(44.731171, 36.778259, 37.964252, 43.653225),
                       lon = c(-75.428833, -119.417931, -91.831833, -79.383186))

  res <- address_reverse_census_all(coords)

  expect_equal(res$state_fips, c("36", "06", "29", NA))
  expect_equal(res$county_fips, c("089", "019", "161", NA))
  expect_named(res, c("state_fips","county_fips","census_tract","census_block","lat","lon"),
               ignore.order = TRUE)
  expect_equal(nrow(coords),nrow(res))

  coords <- data.table(lat = c(43.653225),
                       lon = c(-79.383186))

  res <- address_reverse_census_all(coords)

  expect_named(res, c("lat","lon"), ignore.order = TRUE)
  expect_equal(nrow(coords),nrow(res))

})

test_that("address_reverse_census_all is resilient to API failures",{
  stub(address_reverse_census_all, "cxy_geography", \(...) stop("Census API Error!"))
  # NY, CA, MO, Toronto
  coords <- data.table(lat = c(44.731171, 36.778259, 37.964252, 43.653225),
                       lon = c(-75.428833, -119.417931, -91.831833, -79.383186))

  expect_warning(res <- address_reverse_census_all(coords), "Error in cxy_geography.+4 rows.+Census API Error!")

  expect_equal(nrow(coords),nrow(res))

})


# address_reverse_census --------------------------------------------------

test_that("address_reverse_census filters out non-US addresses based on lat/lon",{

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave","30 Churchill Pl"),
    street2 = NA,
    city = c("Brooklyn","London"),
    state = c("NY",NA),
    country = c("USA","UK"),
    postal_code = c("11217","E14 5EU")
  )

  address_geocode <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))[address_stream,on = address_cols]
  address_reverse_census_all <- mock(address_geocode[,.(lat,lon,state_fips,county_fips,census_tract = "address_reverse_census",census_block)])
  address_geocode$census_tract <- NA

  stub(address_reverse_census,"address_reverse_census_all",address_reverse_census_all)
  stub(address_reverse_census,"address_geocode",address_geocode)
  stub(address_reverse_census,"address_cache_chunked",function(..., parallel){address_cache_chunked(..., parallel = FALSE)})

  expect_message(res <- address_reverse_census(address_stream))
  expect_equal(nrow(mock_args(address_reverse_census_all)[[1]][[1]]),1)
  expect_equal(res$census_tract, c("address_reverse_census",NA))

})


test_that("address_reverse_census combines census data from address_geocode and address_reverse_census caches" ,{

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave","321 Ashland Pl"),
    street2 = NA,
    city = "Brooklyn",
    state = "NY",
    country = "USA",
    postal_code = "11217"
  )

  address_geocode <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))[address_stream,on = address_cols]
  address_reverse_census_all <- mock(address_geocode[2,.(lat,lon,state_fips,county_fips,census_tract = "oops",census_block)])
  address_geocode[street1 == "30 Lafayette Ave", census_tract := NA]
  address_geocode[street1 == "321 Ashland Pl", census_tract := "address_geocode"]

  stub(address_reverse_census,"address_reverse_census_all",address_reverse_census_all)
  stub(address_reverse_census,"address_geocode",address_geocode)

  expect_message(res <- address_reverse_census(address_stream))
  expect_length(mock_args(address_reverse_census_all),0)
  expect_equal(res$census_tract, c("address_reverse_census","address_geocode"))

})

test_that("address_reverse_census doesn't call address_geocode if it's already passed in" ,{

  address_stream_already_geocoded <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))
  address_reverse_census_all <- mock(address_geocode[2,.(lat,lon,state_fips,county_fips,census_tract = "oops",census_block)])

  address_geocode <- mock()
  stub(address_reverse_census,"address_reverse_census_all",address_reverse_census_all)
  stub(address_reverse_census,"address_geocode",address_geocode)

  expect_message(res <- address_reverse_census(address_stream_already_geocoded))
  expect_length(mock_args(address_geocode),0)

})

