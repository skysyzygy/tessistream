withr::local_package("mockery")
withr::local_package("dplyr")

audit <- readRDS(test_path("address_audit.Rds"))
addresses <- readRDS(test_path("addresses.Rds"))

test_that("address_create_stream builds a stream using all data from audit table", {
  stub(address_load_audit,"read_tessi",audit)
  stub(address_create_stream,"address_load",addresses[1])
  stub(address_create_stream,"address_load_audit",address_load_audit)
  stub(address_create_stream,"address_fill_debounce_stream",return)
  expect_equal(nrow(address_create_stream()),
               nrow(distinct(audit[column_updated %in% address_cols,.(alternate_key,date)]))+1)
})

test_that("address_create_stream builds a stream using all data from address table", {
  stub(address_load,"read_tessi",addresses)
  stub(address_load_audit,"read_tessi",audit[column_updated %in% address_cols][1])
  stub(address_create_stream,"address_load",address_load)
  stub(address_create_stream,"address_load_audit",address_load_audit)
  stub(address_create_stream,"address_fill_debounce_stream",return)
  expect_equal(nrow(address_create_stream()),2*length(unique(addresses$address_no))+1)
})

test_that("address_create_stream has data in each row, including creations", {
  stub(address_load,"read_tessi",addresses)
  stub(address_load_audit,"read_tessi",audit)
  stub(address_create_stream,"address_load",address_load)
  stub(address_create_stream,"address_load_audit",address_load_audit)

  stream <- address_create_stream()
  missing_data <- stream[purrr::reduce(lapply(mget(address_cols),is.na),`&`)]

  expect_equal(nrow(missing_data),0)
})

test_that("address_create_stream fills in all data", {
  stub(address_load,"read_tessi",addresses)
  stub(address_load_audit,"read_tessi",audit)
  stub(address_create_stream,"address_load",address_load)
  stub(address_create_stream,"address_load_audit",address_load_audit)

  stream <- address_create_stream()
  aa <- address_load_audit()
  a <- address_load()
  audit[,address_no:=as.integer(alternate_key)]

  missing_data <- data.table::melt(stream,measure.vars=address_cols)[is.na(value)]
  # none of these data exist in the audit table
  expect_equal(aa[missing_data,,on=c("address_no","column_updated"="variable")][!is.na(timestamp),.N],0)

  # none of these data exist as the latest address data
  latest_data <- data.table::melt(a,measure.vars=address_cols)
  missing_data <- latest_data[missing_data,,on=c("address_no","variable")][is.na(timestamp)]

  # unless the address has been deleted...
  expect_equal(missing_data[!audit[action=="Deleted"],.N,on=c("address_no")],0)

})

test_that("address_create_stream returns only one address change per day", {
  stub(address_load,"read_tessi",addresses)
  stub(address_load_audit,"read_tessi",audit)
  stub(address_create_stream,"address_load",address_load)
  stub(address_create_stream,"address_load_audit",address_load_audit)

  stream <- address_create_stream()

  stream[,day:=lubridate::floor_date(timestamp,"day")]
  expect_equal(stream[,.N,by=c("day","address_no")][N>1,.N],0)
})
