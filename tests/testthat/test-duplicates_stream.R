withr::local_package("checkmate")
withr::local_package("mockery")

test_that("duplicates_data returns lowercased, trimmed data", {
  stub(duplicates_data, "read_tessi", readRDS(rprojroot::find_testthat_root_file("duplicates_stream-customers.Rds")))
  stub(duplicates_data, "read_cache", readRDS(rprojroot::find_testthat_root_file("address_stream.Rds")))
  stub(duplicates_data, "stream_from_audit", mock(readRDS(rprojroot::find_testthat_root_file("duplicates_stream-phones.Rds")),
                                                  readRDS(rprojroot::find_testthat_root_file("duplicates_stream-emails.Rds"))))
  stub(duplicates_data, "address_parse", \(...) address_parse(..., db_name = rprojroot::find_testthat_root_file("address_stream.sqlite")))

  debugonce(duplicates_data)
  duplicates_data <- duplicates_data()

  expect_names(duplicates_data, permutation_of = c("customer_no", "fname", "lname",
                                                   "house_number", "road", "unit", "city", "state",
                                                   "postcode", "house", "po_box", "country",
                                                   "email", "phone"))

  expect_equal(lapply(duplicates_data,tolower),duplicates_data)
  expect_equal(lapply(duplicates_data,trimws),duplicates_data)
})
