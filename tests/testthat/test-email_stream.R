withr::local_package("mockery")
withr::local_package("checkmate")

.acq_dt <- lubridate::as_datetime(lubridate::today())

# email_data --------------------------------------------------------------

email_data_stubbed <- function() {
  promotions <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotions.parquet"), as_data_frame = FALSE)
  promotion_responses <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"), as_data_frame = FALSE)
  read_tessi <- mock(promotions, promotion_responses)

  stub(email_data, "read_tessi", read_tessi)

  email_data()
}

test_that("email_data loads from promotions and promotion_responses and returns an arrow table", {
  promotions <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotions.parquet"), as_data_frame = FALSE)
  promotion_responses <- arrow::read_parquet(rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"), as_data_frame = FALSE)
  expect_class(email_data_stubbed(), "ArrowTabular")
  expect_equal(nrow(email_data_stubbed()), nrow(promotions) + nrow(promotion_responses))
})

# email_data_append -------------------------------------------------------

email_data_append_stubbed <- function(email_data) {
  responses <- arrow::arrow_table(response = 1:5, event_subtype = letters[1:5])
  sources <- arrow::arrow_table(source_no = 1:1000, source_desc = cli::hash_md5(1:1000), acq_dt = .acq_dt)
  extractions <- arrow::arrow_table(source_no = 1:1000, extraction_desc = cli::hash_md5(paste("extraction",1:1000)))
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.RDs"))

  read_sql <- mock(responses, extractions)
  read_tessi <- mock(sources)
  stream_from_audit <- mock(emails)

  stub(email_data_append, "read_tessi", read_tessi)
  stub(email_data_append, "read_sql", read_sql)
  stub(email_data_append, "stream_from_audit", stream_from_audit)

  email_data_append(email_data)
}

test_that("email_data_append appends descriptive info for responses, sources, and extractions", {
  withr::local_package("dplyr")

  email_data_append <- email_data_append_stubbed(email_data_stubbed())
  expect_class(email_data_append,"ArrowTabular")
  email_data_append <- collect(email_data_append)

  expect_equal(n_distinct(email_data_append$event_subtype),6) # number of responses plus 1
  expect_equal(n_distinct(email_data_append$source_desc), # one description per source
               n_distinct(email_data_append$source_no))
  expect_equal(n_distinct(email_data_append$extraction_desc), # one description per source
               n_distinct(email_data_append$source_no))

})

test_that("email_data_append recalculates send timestamps based on earliest promote_dt / acq_dt", {
  email_data_append <- email_data_append_stubbed(email_data_stubbed()) %>% collect() %>% setDT
  email_data <- email_data_stubbed() %>% collect() %>% setDT()

  # Doesn't update response times
  email_compare_responses <- merge(
        email_data[!is.na(response),.(customer_no, source_no, timestamp)],
        email_data_append[event_subtype != "Send",.(customer_no, source_no, timestamp)],
        by = c("customer_no", "source_no")
  )

  expect_equal(email_compare_responses[,.(customer_no, source_no, timestamp = timestamp.x)] %>%
                 setkey(customer_no, source_no, timestamp),
               email_compare_responses[,.(customer_no, source_no, timestamp = timestamp.y)] %>%
                 setkey(customer_no, source_no, timestamp))

  # Does update promote dates
  first_responses <- email_data[,.(first_response = min(timestamp, na.rm=T)), by = "source_no"]

  email_compare_sends <- merge(
    first_responses,
    email_data_append[event_subtype == "Send",.(customer_no, source_no, timestamp)],
    by = c("source_no")
  )

  # to first response date
  expect_equal(email_compare_sends[source_no>1,timestamp],
               email_compare_sends[source_no>1,first_response])
  # or acq_dt
  expect_equal(email_compare_sends[source_no==1,timestamp],
               rep(.acq_dt,email_compare_sends[source_no==1,.N])) # variable set above

})

test_that("email_data_append fills in customer emails based on the send date", {
  email_data_append <- email_data_append_stubbed(email_data_stubbed()) %>% collect() %>% setDT
  emails <- readRDS(rprojroot::find_testthat_root_file("email_stream-emails.RDs")) %>% setDT
  email_data <- email_data_stubbed() %>% collect() %>% setDT()

  expect_gt(email_data_append[!is.na(eaddress),.N],email_data[!is.na(eaddress),.N])

})


# email_stream ------------------------------------------------------------

test_that("email_stream is sane", {
  stub(email_stream,"email_data",email_data_stubbed)
  stub(email_stream,"email_data_append",email_data_append_stubbed)

  email_stream <- email_stream()

  # all rows have a customer
  expect_equal(email_stream[is.na(group_customer_no),.N],0)
  # # Check that there is time information on most promotions and responses
  # # TEST: all rows have time information
  # testit::assert(emailStream[is.na(timestamp),] %>% nrow == 0)
  # # TEST: 98% of all rows have an email address...
  # testit::assert(emailStream[is.na(domain)] %>% nrow/nrow(emailStream)<.02)
  # #Clean up

})

