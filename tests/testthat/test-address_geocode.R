withr::local_package("checkmate")
withr::local_package("mockery")

tessilake:::local_cache_dirs()


#stub(address_geocode_census,"address_exec_census",function(.){dplyr::mutate(.,cxy_quality="Oops")})

# address_exec_census -----------------------------------------------------

address_stream <- data.table(census.street="30 Lafayette Ave",
                             census.city="Brooklyn",
                             census.state="NY",
                             census.postal_code="11217")

test_that("address_exec_census handles multiple vintages in calls to cxy_geocode", {
  expect_silent(address_exec_census(cbind(address_stream,census.vintageName=c("Current_Current","Census2020_Current"))))

  cxy_geocode <- mock(address_stream,cycle=TRUE)
  stub(address_exec_census,"cxy_geocode",cxy_geocode)
  address_exec_census(cbind(address_stream,census.vintageName=rep(c("Current_Current","Census2020_Current"),5)))

  expect_equal(length(mock_args(cxy_geocode)),2)
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]),5)
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]),5)
})

test_that("address_exec_census handles returns a single data.table with all columns from address_stream", {
  result <- address_exec_census(cbind(address_stream,test="something",census.vintageName=c("Current_Current","Census2020_Current")))

  expect_true(all(c(colnames(address_stream),"census.vintageName","test") %in% colnames(result)))
  expect_class(result,"data.table")
})


# address_geocode_census --------------------------------------------------

address_stream <- data.table(street1="30 Lafayette Ave",
                             street2="Brooklyn Academy of Music",
                             city="Brooklyn",
                             state="NY",
                             country="USA",
                             postal_code="11217")

rm(address_geocode_census)

test_that("address_geocode_census adds census vintages to address_stream",{
  stub(address_geocode_census,"setleftjoin",function(...){rlang::abort(data=setleftjoin(...),class="setleftjoin")})
  cnd <- expect_error(address_geocode_census(cbind(address_stream,
                                                   timestamp=seq(lubridate::ymd("2000-01-01"),lubridate::ymd("2022-01-01"),by="year"))),class="setleftjoin")
  expect_names(colnames(cnd$data),must.include="vintageName")
  expect_equal(cnd$data$vintageName,paste0(c(rep("Census2010",11),
                                             rep("ACS2017",7),
                                             "ACS2018",
                                             "ACS2019",
                                             "Census2020",
                                             "ACS2021",
                                             "ACS2022"),"_Current"))

  expect_false(any(is.na(cnd$data$vintageName)))

})

test_that("address_geocode_census adds parsing to address_stream if it exists",{
  stub(address_geocode_census,"address_exec_census",function(.){dplyr::mutate(.,cxy_quality="Oops")})

  stub(address_geocode_census,"setleftjoin",function(...){rlang::inform(data=setleftjoin(...),class="setleftjoin")})
  cnd <- expect_message(address_geocode_census(cbind(address_stream,timestamp=lubridate::now())),class="setleftjoin")

  address_stream_parsed <- function(address_stream){cbind(address_stream,libpostal=data.table(house_number="30",road="Lafaytte Ave",city="Brooklyn",state="NY",postcode="11217"))}
  address_cache(address_stream,"address_parse",address_stream_parsed)

  cnd <- expect_message(address_geocode_census(address_stream[,timestamp:=lubridate::now()]),class="setleftjoin")
  expect_names(colnames(cnd$data),must.include=paste0("libpostal.",c("house_number","road","city","state","postcode")))
})

test_that("address_geocode_census only sends US addresses to census parser",{
  cxy_geocode <- mock(cbind(rlang::env_get(rlang::caller_env(3),".SD"),cxy_quality="Really bad"),cycle=T)
  stub(address_exec_census,"cxy_geocode",cxy_geocode)
  stub(address_geocode_census,"address_exec_census",address_exec_census)

  address_stream <- expand.grid(street1="30 Lafayette Ave",
              street2="Brooklyn Academy of Music",
              city="Brooklyn",
              state=c("NY",NA_character_,"123"),
              country=c("USA","UK"),
              postal_code=c("11217","ABCDE","1","00000")) %>% lapply(as.character) %>% setDT

  result <- address_geocode_census(address_stream[,timestamp:=lubridate::now()])

  expect_length(mock_args(cxy_geocode),3)
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]),1) # libpostal only matches the first one
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]),2) # street1 matches USA & UK
  expect_equal(nrow(mock_args(cxy_geocode)[[3]][[1]]),2) # street2 matches USA & UK

  expect_equal(nrow(result),0) # nothing successful

})

test_that("address_geocode_census does not retry successful geocodes",{
  cxy_geocode <- mock(cbind(rlang::env_get(rlang::caller_env(3),".SD"),cxy_quality="Exact"),cycle=T)
  stub(address_exec_census,"cxy_geocode",cxy_geocode)
  stub(address_geocode_census,"address_exec_census",address_exec_census)

  address_stream <- expand.grid(street1=c("30 Lafayette Ave","651 Fulton St"),
                                street2="Brooklyn Academy of Music",
                                city="Brooklyn",
                                country="USA",
                                state=c("NY","New York"),
                                postal_code="11217") %>% lapply(as.character) %>% setDT

  result <- address_geocode_census(address_stream[,timestamp:=lubridate::now()])

  expect_length(mock_args(cxy_geocode),2)
  expect_equal(nrow(mock_args(cxy_geocode)[[1]][[1]]),1) # libpostal only matches the first one
  expect_equal(nrow(mock_args(cxy_geocode)[[2]][[1]]),3) # the three that haven't been tried yet
  #expect_equal(nrow(mock_args(cxy_geocode)[[3]][[1]]),0) # nothing left to do!

  expect_equal(nrow(result),nrow(address_stream))

})
