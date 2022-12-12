withr::local_package("checkmate")
withr::local_package("mockery")

tessilake:::local_cache_dirs()

# address_exec_census -----------------------------------------------------

address_stream <- data.table(
  street = "30 Lafayette Ave",
  city = "Brooklyn",
  state = "NY",
  postal_code = "11217"
)

test_that("address_exec_census handles multiple vintages in calls to cxy_geocode", {
  expect_silent(address_exec_census(cbind(address_stream, vintageName = c("Current_Current", "Census2020_Current"))))

  cxy_geocode <- mock(address_stream, cycle = TRUE)
  stub(address_exec_census, "cxy_geocode", cxy_geocode)
  address_exec_census(cbind(address_stream, vintageName = rep(c("Current_Current", "Census2020_Current"), 5)))

  expect_equal(length(mock_args(cxy_geocode)), 2)
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]), 5)
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]), 5)
})

test_that("address_exec_census handles returns a single data.table with all columns from address_stream", {
  result <- address_exec_census(cbind(address_stream, test = "something", vintageName = c("Current_Current", "Census2020_Current")))

  expect_true(all(c(colnames(address_stream), "vintageName", "test") %in% colnames(result)))
  expect_class(result, "data.table")
})


# address_geocode_census --------------------------------------------------

address_stream <- data.table(
  street1 = "30 Lafayette Ave",
  street2 = "Brooklyn Academy of Music",
  city = "Brooklyn",
  state = "NY",
  country = "USA",
  postal_code = "11217"
)

test_that("address_geocode_census adds census vintages to address_stream", {
  stub(address_geocode_census, "setleftjoin", function(...) {
    rlang::abort(data = setleftjoin(...), class = "setleftjoin")
  })
  cnd <- expect_error(address_geocode_census(cbind(address_stream,
    timestamp = seq(lubridate::ymd("2000-01-01"), lubridate::ymd("2022-01-01"), by = "year")
  )), class = "setleftjoin")
  expect_names(colnames(cnd$data), must.include = "vintageName")
  expect_equal(cnd$data$vintageName, paste0(c(
    rep("Census2010", 11),
    rep("ACS2017", 7),
    "ACS2018",
    "ACS2019",
    "Census2020",
    "ACS2021",
    "ACS2022"
  ), "_Current"))

  expect_false(any(is.na(cnd$data$vintageName)))
})

test_that("address_geocode_census adds parsing to address_stream if it exists", {
  stub(address_geocode_census, "address_exec_census", function(.) {
    dplyr::mutate(., cxy_quality = "Oops")
  })

  debugonce(address_geocode_census)
  stub(address_geocode_census, "setleftjoin", function(...) {
    rlang::inform(data = setleftjoin(...), class = "setleftjoin")
  })
  cnd <- expect_message(address_geocode_census(cbind(address_stream, timestamp = lubridate::now())), class = "setleftjoin")

  address_stream_parsed <- function(address_stream) {
    cbind(address_stream, libpostal = data.table(house_number = "30", road = "Lafayette Ave", city = "Libpostal", state = "NY", postcode = "11217"))
  }
  address_cache(address_stream, "address_parse", address_stream_parsed)

  cnd <- expect_message(expect_message(address_geocode_census(address_stream[, timestamp := lubridate::now()]), class = "setleftjoin"), class = "setleftjoin")
  expect_names(colnames(cnd$data), must.include = paste0("libpostal.", c("house_number", "road", "city", "state", "postcode")))
})

test_that("address_geocode_census doesn't send duplicate addresses", {
  cxy_geocode <- mock(cbind(rlang::env_get(rlang::caller_env(3), ".SD"), cxy_quality = "Really bad"), cycle = TRUE)
  stub(address_exec_census, "cxy_geocode", cxy_geocode)
  stub(address_geocode_census, "address_exec_census", address_exec_census)

  address_stream <- data.table(
    street1 = rep("30 Lafayette Ave", 3),
    street2 = "Brooklyn Academy of Music",
    city = "Brooklyn",
    state = "NY",
    country = "USA",
    postal_code = "11217"
  ) %>% setDT()

  address_geocode_census(address_stream[, timestamp := lubridate::now()])

  expect_length(mock_args(cxy_geocode), 3) # street1 & street2, libpostal
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]), 1)
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]), 1) # Each gets sent only once
  expect_equal(nrow(mock_args(cxy_geocode)[[3]][[1]]), 1)
})

test_that("address_geocode_census only sends US addresses to census parser", {
  cxy_geocode <- mock(cbind(rlang::env_get(rlang::caller_env(3), ".SD"), cxy_quality = "Really bad"), cycle = TRUE)
  stub(address_exec_census, "cxy_geocode", cxy_geocode)
  stub(address_geocode_census, "address_exec_census", address_exec_census)

  address_stream <- expand.grid(
    street1 = "30 Lafayette Ave",
    street2 = "Brooklyn Academy of Music",
    city = "Brooklyn",
    state = c("NY", NA_character_, "123"),
    country = c("USA", "UK"),
    postal_code = c("11217", "1121712345", "ABCDE", "1", "00000")
  ) %>%
    lapply(as.character) %>%
    setDT()

  result <- address_geocode_census(address_stream[, timestamp := lubridate::now()])

  expect_length(mock_args(cxy_geocode), 3)
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]), 1) # libpostal only matches the first one
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]), 2) # street1 x 11217 & 1121712345
  expect_equal(nrow(mock_args(cxy_geocode)[[3]][[1]]), 2) # street2 x 11217 & 1121712345

  expect_equal(nrow(result), 9) # USA & UK for street1/street2 + 1 for libpostal
})

test_that("address_geocode_census does not retry successfully geocoded addresses", {
  cxy_geocode <- mock(cbind(rlang::env_get(rlang::caller_env(3), ".SD"), cxy_quality = "Exact"), cycle = TRUE)
  stub(address_exec_census, "cxy_geocode", cxy_geocode)
  stub(address_geocode_census, "address_exec_census", address_exec_census)

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

  result <- address_geocode_census(address_stream[, timestamp := lubridate::now()])


  expect_length(mock_args(cxy_geocode), 2)
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]), 1) # libpostal only matches the first address
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]), 3) # the three addresses that haven't been tried yet (libpostal is different)
  # expect_equal(nrow(mock_args(cxy_geocode)[[3]][[1]]),0) # nothing left to do!

  expect_equal(nrow(result), nrow(address_stream))
})
