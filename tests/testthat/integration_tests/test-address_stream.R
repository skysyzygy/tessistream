# Runs a full test using the fixture created by address_prepare_fixtures() as a small address_stream
# Requires access to Tessi and saves/accesses the sqlite file in the deep stream directory

withr::local_envvar(R_CONFIG_FILE="")
withr::local_package("progressr")
future::plan("multisession")
handlers("cli")
mockery::stub(address_stream, "address_create_stream", readRDS(rprojroot::find_testthat_root_file("address_stream.Rds")))
with_progress(address_stream()) %>% collect %>% setDT
address_stream <- read_cache("address_stream","deep","stream") %>% collect %>% setDT
address_stream_full <- read_cache("address_stream_full","deep","stream") %>% collect %>% setDT


test_that("99.9% of customers have a primary address", {
  expect_gte(address_stream_full[,any(primary_ind=="Y"),by="group_customer_no"][V1==T,.N] /
            distinct(address_stream_full,group_customer_no)[,.N], .999)
})
test_that("99.5% of valid addresses have all address data", {
  # non-US addresses are sometimes missing state info...
  address_stream_full <- address_stream_full[!grepl("web add|unknown|na|^$",street1,ignore.case=T) &
                                             !is.na(street1)]
  address_stream_full[country!="USA" & is.na(state), state:="state"]
  expect_gte(address_stream_full[,sum(apply(!is.na(.SD),1,all)),.SDcols=address_cols[-2]] /
               address_stream_full[,.N], .995)
  address_stream <- address_stream[!grepl("web add|unknown|na|^$",street1,ignore.case=T) &
                                   !is.na(street1)]
  address_stream[country!="USA" & is.na(state), state:="state"]
  expect_gte(address_stream[,sum(apply(!is.na(.SD),1,all)),.SDcols=address_cols[-2]] /
               address_stream[,.N], .995)
})
test_that("99.5% of valid addresses have a geocode", {
  address_stream_full <- address_stream_full[!grepl("web add|unknown|na|^$",street1,ignore.case=T) &
                                             !is.na(street1)]
  expect_gte(address_stream_full[!is.na(lat) & !is.na(lon),.N] /
               address_stream_full[,.N], .995)
  address_stream <- address_stream[!grepl("web add|unknown|na|^$",street1,ignore.case=T) &
                                     !is.na(street1)]
  expect_gte(address_stream[!is.na(lat) & !is.na(lon),.N] /
               address_stream[,.N], .995)
})
test_that("99.5% of geocoded US addresses have census data", {
  expect_gte(address_stream_full[!is.na(address_median_income_level),.N] /
               address_stream_full[!is.na(lat) & !is.na(lon) & country == "USA", .N], .995)
  expect_gte(address_stream[!is.na(address_median_income_level),.N] /
               address_stream[!is.na(lat) & !is.na(lon) & country == "USA",.N], .995)
})
test_that("iWave data has been appended to the correct records", {
  iwave_customers <- read_tessi("iwave", "customer_no") %>% collect()
  expect_gt(address_stream_full[!is.na(address_pro_score_level),.N] /
            address_stream_full[group_customer_no %in% iwave_customers$group_customer_no & timestamp>='2018-07-01', .N], .9)
  expect_lt(address_stream_full[!is.na(address_pro_score_level),.N] /
              address_stream_full[group_customer_no %in% iwave_customers$group_customer_no & timestamp>='2018-07-01', .N], 1.1)
})


