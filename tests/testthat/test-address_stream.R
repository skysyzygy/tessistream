withr::local_package("mockery")
withr::local_package("dplyr")

audit <- readRDS(test_path("address_audit.Rds"))
addresses <- readRDS(test_path("addresses.Rds"))
# Use this to turn off libpostal execution
stub(address_exec_libpostal,"Sys.which","")

# address_create_stream ---------------------------------------------------

test_that("address_create_stream builds a stream using all data from audit table", {
  stub(address_load_audit, "read_tessi", audit)
  stub(address_create_stream, "address_load", addresses[1])
  stub(address_create_stream, "address_load_audit", address_load_audit)
  stub(address_create_stream, "address_fill_debounce_stream", return)
  expect_equal(
    nrow(address_create_stream()),
    nrow(distinct(audit[column_updated %in% c(address_cols,"primary_ind"), .(alternate_key, date)])) + 1
  )
})

test_that("address_create_stream builds a stream using all data from address table", {
  stub(address_load, "read_tessi", addresses)
  stub(address_load_audit, "read_tessi", audit[column_updated %in% address_cols][1])
  stub(address_create_stream, "address_load", address_load)
  stub(address_create_stream, "address_load_audit", address_load_audit)
  stub(address_create_stream, "address_fill_debounce_stream", return)
  expect_equal(nrow(address_create_stream()), 2 * length(unique(addresses$address_no)) + 1)
})

test_that("address_create_stream has data in each row, including creations", {
  stub(address_load, "read_tessi", addresses)
  stub(address_load_audit, "read_tessi", audit)
  stub(address_create_stream, "address_load", address_load)
  stub(address_create_stream, "address_load_audit", address_load_audit)

  stream <- address_create_stream()
  missing_data <- stream[purrr::reduce(lapply(mget(address_cols), is.na), `&`)]

  expect_equal(nrow(missing_data), 0)
})

test_that("address_create_stream fills in all data", {
  stub(address_load, "read_tessi", addresses)
  stub(address_load_audit, "read_tessi", audit)
  stub(address_create_stream, "address_load", address_load)
  stub(address_create_stream, "address_load_audit", address_load_audit)

  stream <- address_create_stream()
  aa <- address_load_audit()
  a <- address_load()
  audit[, address_no := as.integer(alternate_key)]

  missing_data <- data.table::melt(stream, measure.vars = address_cols)[is.na(value)]
  # none of these data exist in the audit table
  expect_equal(aa[missing_data, , on = c("address_no", "column_updated" = "variable")][!is.na(timestamp), .N], 0)

  # none of these data exist as the latest address data
  latest_data <- data.table::melt(a, measure.vars = address_cols)
  missing_data <- latest_data[missing_data, , on = c("address_no", "variable")][is.na(timestamp)]

  # unless the address has been deleted...
  expect_equal(missing_data[!audit[action == "Deleted"], .N, on = c("address_no")], 0)
})

test_that("address_create_stream returns only one address change per day", {
  stub(address_load, "read_tessi", addresses)
  stub(address_load_audit, "read_tessi", audit)
  stub(address_create_stream, "address_load", address_load)
  stub(address_create_stream, "address_load_audit", address_load_audit)

  stream <- address_create_stream()

  stream[, day := lubridate::floor_date(timestamp, "day")]
  expect_equal(stream[, .N, by = c("day", "address_no")][N > 1, .N], 0)
})

# address_clean -----------------------------------------------------------

test_that("address_clean removes tabs, newlines, and multiple spaces", {
  spaces <- c("one\twhitespace", "two\n\rtogether", "several\rin\tdifferent places", "lots    of    spaces")
  cleaned <- c("one whitespace", "two together", "several in different places", "lots of spaces")
  address_stream <- do.call(data.table, setNames(rep(list(spaces), length(address_cols)), address_cols))
  address_stream_cleaned <- do.call(data.table, setNames(rep(list(cleaned), length(address_cols)), address_cols))

  expect_equal(address_clean(address_stream), address_stream_cleaned)
})

test_that("address_clean trims whitespace and lowercases", {
  spaces <- c(" SPACE be-4", "spA(e aft`r ", "\r\t\nand some other things!  ")
  cleaned <- c("space be-4", "spa(e aft`r", "and some other things!")
  address_stream <- do.call(data.table, setNames(rep(list(spaces), length(address_cols)), address_cols))
  address_stream_cleaned <- do.call(data.table, setNames(rep(list(cleaned), length(address_cols)), address_cols))

  expect_equal(address_clean(address_stream), address_stream_cleaned)
})

test_that("address_clean removes junk info", {
  # replaced with NA
  junk_type_1 <- c("unknown", "Web Added", "NO ADDRESS")
  # row removed
  junk_type_2 <- setNames(list("30 Lafayette Ave.", "Development", "Brooklyn", "NY", "11217-0000", ""), address_cols)
  cleaned <- c(NA_character_, NA_character_, NA_character_)
  address_stream <- do.call(data.table, setNames(rep(list(junk_type_1), length(address_cols)), address_cols))
  address_stream <- rbind(address_stream, junk_type_2)
  address_stream_cleaned <- do.call(data.table, setNames(rep(list(cleaned), length(address_cols)), address_cols))

  expect_equal(address_clean(address_stream), address_stream_cleaned)
})

# address_exec_libpostal --------------------------------------------------

test_that("address_exec_libpostal complains if addresses are not a character vector",{
  expect_error(address_exec_libpostal(c(1,2,3,4)),"addresses.+character")
  expect_error(address_exec_libpostal(NA),"addresses.+missing")
})
test_that("address_exec_libpostal complains if libpostal can't be found",{
  stub(address_exec_libpostal,"Sys.which","")
  expect_error(address_exec_libpostal("test"),"address_parser.+not found")
})
test_that("address_exec_libpostal gets back data from libpostal",{
  expect_mapequal(address_exec_libpostal("Brooklyn Academy of Music, 30 lafayette ave, brooklyn, ny 11217"),
               data.frame(house_number="30",
                          road="lafayette ave",
                          house="brooklyn academy of music",
                          city_district="brooklyn",
                          state="ny",
                          postcode="11217"))
})
test_that("address_exec_libpostal handles UTF-8",{
  expect_mapequal(address_exec_libpostal("92 Ave des Champs-Élysées"),
                  data.frame(house_number="92",
                             road="ave des champs-élysées"))
})

# address_parse_libpostal ------------------------------------------------

test_that("address_parse_libpostal sends a distinct, lowercase character vector to libpostal",{
  libpostal <- mock(rlang::abort(class="libpostal"))
  stub(address_parse_libpostal,"address_exec_libpostal",libpostal)

  address_stream <- expand.grid(street1=c("Brooklyn Academy of Music","30 Lafayette Ave"),
                           street2=c("30 Lafeyette Ave","4th Floor"),
                           city=c("Brooklyn","NY"),
                           state="NY",
                           postal_code="11217",
                           country="",
                           primary_ind="Y") %>% rbind(.,.) %>% setDT

  expect_error(address_parse_libpostal(address_stream),class = "libpostal")

  expect_equal(length(mock_args(libpostal)[[1]][[1]]),8)
  expect_equal(mock_args(libpostal)[[1]][[1]],tolower(mock_args(libpostal)[[1]][[1]]))
  expect_equal(mock_args(libpostal)[[1]][[1]],unique(mock_args(libpostal)[[1]][[1]]))

})

test_that("address_parse_libpostal handles unit #s hidden in postalcode, street2, and road",{
  address_stream <- data.table()[,(address_cols) := rep(NA,8)][,`:=`(street1=1:8,
                                                                     street2=c("4k",rep(NA,7)),
                                                                     postal_code = rep("11217",8))]

  libpostal_return <- data.table( "road"=c(rep("lafayette ave",7),"lafayette ave w apt 4g"),
                                  "unit"=rep(NA_character_,8),
                                  "postcode"=c(NA,"11217","4g","33","a4","ph33","fl. 4",NA))

  stub(address_parse_libpostal,"address_exec_libpostal",libpostal_return)

  parsed <- address_parse_libpostal(address_stream)

  expect_equal(parsed$libpostal.postcode,c(NA,"11217",rep(NA,6)))
  expect_equal(parsed$libpostal.unit,c("4k",NA,"4g","33","a4","ph33","fl. 4","apt 4g"))
  expect_equal(parsed$libpostal.road,c(rep("lafayette ave",7),"lafayette ave w"))

})
test_that("address_parse_libpostal cleans up duplicated unit #s",{
  address_stream <- data.table()[,(address_cols) := rep(NA,8)][,`:=`(street1=1:8)]

  libpostal_return <- data.table( "road"=c("lafayette ave","lafayette st e"),
                                  "unit"=c("4k","","4g","33","a4","ph33","fl. 4","apt 4g"),
                                  "postcode"=c(NA,"11217","4g","11233","a4","ph33","fl. 4",NA)) %>%
    .[,`:=`(road = trimws(paste(road,unit)),
            house = unit)]

  stub(address_parse_libpostal,"address_exec_libpostal",libpostal_return)

  parsed <- address_parse_libpostal(address_stream)

  expect_equal(parsed$libpostal.postcode,c(NA,"11217",NA,"11233",rep(NA,4)))
  expect_equal(parsed$libpostal.unit,c("4k",NA,"4g","33","a4","ph33","fl. 4","apt 4g"))
  expect_equal(parsed$libpostal.house,rep(NA_character_,8))
  expect_equal(parsed$libpostal.road,rep(c("lafayette ave","lafayette st e"),4))
})
test_that("address_parse_libpostal returns only house_number,road,unit,house,po_box,city,state,country,postcode but handles all columns",{
  libpostal_cols <- c("house", "category", "near", "house_number", "road", "unit", "level", "staircase",
                      "entrance", "po_box", "postcode", "suburb", "city_district", "city", "island",
                      "state_district", "state", "country_region", "country", "world_region")

  libpostal_return <- structure(libpostal_cols,names=libpostal_cols) %>% as.list %>% setDT %>% .[rep(1,8)]
  address_stream <- data.table()[,(address_cols) := rep(NA,8)][,`:=`(street1=1:8)]
  stub(address_parse_libpostal,"address_exec_libpostal",libpostal_return)

  parsed <- address_parse_libpostal(address_stream)

  expect_equal(colnames(parsed),c(
    as.character(address_cols),paste0("libpostal.",
                                  c("house_number","road","unit","house","po_box","city","state","country","postcode"))))

  expect_equal(parsed$libpostal.house_number,rep("house_number",8))
  expect_equal(parsed$libpostal.unit,rep(paste("unit","level","entrance","staircase"),8))
  expect_equal(parsed$libpostal.house,rep(paste("house","category","near"),8))
  expect_equal(parsed$libpostal.city,rep(paste("suburb","city_district","city","island",sep=", "),8))
  expect_equal(parsed$libpostal.state,rep(paste("state_district","state",sep=", "),8))
  expect_equal(parsed$libpostal.country,rep(paste("country_region","country","world_region",sep=", "),8))
})

# address_cache -----------------------------------------------------------
tessilake:::local_cache_dirs()

address_stream <- data.table(street1=paste(1:100,"example lane"),something="extra")
address_processor <- function(address_stream) {
  address_stream[,.(street1,processed=strsplit(street1," ",) %>% purrr::map_chr(1))]
}

test_that("address_cache handles cache non-existence and writes a cache file containing only address_processed cols",{
  expect_equal(tessilake:::cache_exists("address_cache","deep","stream"),FALSE)
  expect_message(address_cache(address_stream[1:50],"address_cache",address_processor),"Cache file not found")
  expect_equal(tessilake:::cache_exists("address_cache","deep","stream"),TRUE)
  expect_named(tessilake:::cache_read("address_cache","deep","stream"),c("street1","processed"))
})

test_that("address_cache only send cache misses to .function",{
  address_result <- address_processor(address_stream)
  address_processor <- mock(address_result[51:100])
  address_cache(address_stream,"address_cache",address_processor)
  expect_equal(nrow(mock_args(address_processor)[[1]][[1]]),50)
})

test_that("address_cache updates the the cache file after cache misses",{
  expect_equal(nrow(tessilake:::cache_read("address_cache","deep","stream")),100)
  expect_named(tessilake:::cache_read("address_cache","deep","stream"),c("street1","processed"))
})

test_that("address_parse incremental runs match address_parse full runs",{
  incremental <- tessilake:::cache_read("address_cache","deep","stream") %>% collect
  expect_message(full <- address_cache(address_stream,"address_cache_full",address_processor),"Cache file not found")

  expect_mapequal(incremental,full)
})

# address_parse -----------------------------------------------------------
address_stream_parsed <- data.table(house_number="30",
                                    road="lafayette ave",
                                    house="brooklyn academy of music",
                                    city="brooklyn",
                                    state="ny",
                                    country=NA,
                                    unit=NA,
                                    postcode="11217")

test_that("address_parse handles cache non-existence and writes a cache file containing only address_cols and libpostal cols",{

  address_stream <- data.table(street1="Brooklyn Academy of Music",
                               street2="30 Lafeyette Ave",
                               city="Brooklyn",
                               state="NY",
                               postal_code="11217",
                               country="")

  stub(address_parse_libpostal,"address_exec_libpostal",address_stream_parsed)
  stub(address_parse,"address_parse_libpostal",address_parse_libpostal)

  expect_equal(tessilake:::cache_exists("address_parse","deep","stream"),FALSE)
  expect_message(address_parse(address_stream),"Cache file not found")
  expect_equal(tessilake:::cache_exists("address_parse","deep","stream"),TRUE)
  expect_named(tessilake:::cache_read("address_parse","deep","stream"),
               as.character(c(address_cols,paste0("libpostal.",colnames(address_stream_parsed)))),
               ignore.order = TRUE)

})



test_that("address_parse only send cache misses to address_parse_libpostal",{
  address_stream <- expand.grid(street1=c("Brooklyn Academy of Music","BAM"),
                               street2=c("30 Lafeyette Ave","30 Lafayette"),
                               city=c("Brooklyn","NYC"),
                               state=c("NY","New York"),
                               postal_code=c("11217","00000"),
                               country=c("","US"),
                               # should not be a cache miss
                               primary_ind=c("Y","N")) %>% setDT

  address_stream_parsed <- data.table::rbindlist(rep(list(address_stream_parsed),63))

  address_exec_libpostal <- mock(address_stream_parsed)
  stub(address_parse_libpostal,"address_exec_libpostal",address_exec_libpostal)
  stub(address_parse,"address_parse_libpostal",address_parse_libpostal)
  address_parse(address_stream)
  expect_equal(length(mock_args(address_exec_libpostal)[[1]][[1]]),63)

})

test_that("address_parse updates the the cache file after cache misses",{

  expect_equal(nrow(tessilake:::cache_read("address_parse","deep","stream")),64)
  expect_named(tessilake:::cache_read("address_parse","deep","stream"),
               as.character(c(address_cols,paste0("libpostal.",colnames(address_stream_parsed)))),
               ignore.order = TRUE)
})

test_that("address_parse incremental runs match address_parse full runs",{
  address_stream <- data.table(street1=c("Brooklyn Academy of Music","BAM"),
                               street2="30 Lafeyette Ave",
                               city="Brooklyn",
                               state="NY",
                               postal_code="11217",
                               country="")

  tessilake:::cache_delete("address_parse","deep","stream")

  stub(address_parse_libpostal,"address_exec_libpostal",address_stream_parsed[,house_number])
  stub(address_parse,"address_parse_libpostal",address_parse_libpostal)

  expect_equal(tessilake:::cache_exists("incremental","deep","stream"),FALSE)
  expect_message(address_parse(address_stream[1]),"Cache file not found")

  rm(address_parse_libpostal,address_parse)

  stub(address_parse_libpostal,"address_exec_libpostal",address_stream_parsed)
  stub(address_parse,"address_parse_libpostal",address_parse_libpostal)

  incremental <- address_parse(address_stream)

  rm(address_parse_libpostal,address_parse)
  tessilake:::cache_delete("address_parse","deep","stream")

  stub(address_parse_libpostal,"address_exec_libpostal",rbind(address_stream_parsed[,house_number],address_stream_parsed,fill=T))
  stub(address_parse,"address_parse_libpostal",address_parse_libpostal)

  expect_message(full <- address_parse(address_stream),"Cache file not found")

  expect_mapequal(incremental,full)
})

