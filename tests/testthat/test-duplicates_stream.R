withr::local_package("checkmate")
withr::local_package("mockery")

# duplicates_data ---------------------------------------------------------

test_that("duplicates_data returns data needed for deduplicating", {
  stub(duplicates_data, "read_tessi", readRDS(rprojroot::find_testthat_root_file("duplicates_stream-customers.Rds")))
  stub(duplicates_data, "read_cache", readRDS(rprojroot::find_testthat_root_file("address_stream.Rds")))
  stub(duplicates_data, "stream_from_audit", mock(readRDS(rprojroot::find_testthat_root_file("duplicates_stream-phones.Rds")),
                                                  readRDS(rprojroot::find_testthat_root_file("duplicates_stream-emails.Rds"))))
  stub(duplicates_data, "address_parse", \(...) address_parse(..., db_name = rprojroot::find_testthat_root_file("address_stream.sqlite")))

  duplicates_data <- duplicates_data()

  expect_names(colnames(duplicates_data), permutation.of = c("customer_no", "group_customer_no", "fname", "lname",
                                                             "house_number", "road", "unit", "city", "state",
                                                             "postcode", "house", "po_box", "country",
                                                             "email", "phone"))

  expect_true(all(sapply(duplicates_data[,c("customer_no","group_customer_no")], is.integer)))
  expect_true(all(sapply(duplicates_data[,-c("customer_no","group_customer_no")], is.character)))
})

# duplicates_stream -------------------------------------------------------

test_that("duplicates_stream returns all email matches", {
  tessilake:::local_cache_dirs()
  emails <- paste0(letters,"@me.com")

  duplicates_data <- data.table(email = rep(emails,2), customer_no = seq(52), group_customer_no = seq(52))
  stub(duplicates_stream, "duplicates_data", duplicates_data)

  dupes <- duplicates_stream()
  expect_equal(nrow(dupes), 26)
  expect_mapequal(dupes, data.table(email = emails,
                                    customer_no_match = seq(26), customer_no = seq(27,52),
                                    i.group_customer_no = seq(26), group_customer_no = seq(27,52)))

  duplicates_data <- data.table(email = rep(emails,3), customer_no = seq(78), group_customer_no = seq(78))
  stub(duplicates_stream, "duplicates_data", duplicates_data)

  dupes <- duplicates_stream()
  expect_equal(nrow(dupes), 52)
  expect_equal(dupes$email, rep(emails,2))
  expect_equal(dupes$customer_no_match, seq(52))
  expect_equal(dupes$customer_no, seq(27,78))

})

test_that("duplicates_stream suppresses matches by household", {
  tessilake:::local_cache_dirs()
  emails <- paste0(letters,"@me.com")

  duplicates_data <- data.table(email = rep(emails,2), customer_no = seq(52), group_customer_no = rep(seq(26), 2))
  stub(duplicates_stream, "duplicates_data", duplicates_data)

  dupes <- duplicates_stream()
  expect_equal(nrow(dupes), 0)

})
