withr::local_package("mockery")
withr::local_package("lubridate")

# tessi_changed_emails ----------------------------------------------------
email_stream <- data.table( customer_no = 1,
  eaddress_no = 1,
  timestamp = now(),
  primary_ind = "Y",
  inactive = "N",
  event_subtype = c("Current"),
  address = "a")

merges <- data.table(
  status = "S",
  kept_id = 1,
  delete_id = 0,
  merge_dt = now())

test_that("tessi_changed_emails passes ... on to stream_from_audit and read_sql_table", {
  stream_from_audit <- mock(email_stream)
  read_sql_table <- mock(merges)

  stub(tessi_changed_emails, "stream_from_audit", stream_from_audit)
  stub(tessi_changed_emails, "read_sql_table", read_sql_table)

  tessi_changed_emails(a = "b", c = "d")
  expect_equal(mock_args(stream_from_audit)[[1]][["a"]], "b")
  expect_equal(mock_args(read_sql_table)[[1]][["c"]], "d")
})

test_that("tessi_changed_emails gets all emails changed at a particular point in time", {
  time <- with_tz(now(), Sys.timezone())

  email_stream <- data.table(
    customer_no = 1,
    eaddress_no = 1,
    timestamp = time + dhours(seq(-1, 1)),
    primary_ind = "Y",
    inactive = "N",
    # Debouncing means only one row per timestamp per email
    event_subtype = c("Creation", "Current", "Changed"),
    address = c("a", "b", "b")
  )[c(3, 2, 1)]

  stub(tessi_changed_emails, "stream_from_audit", email_stream)
  stub(tessi_changed_emails, "read_sql_table", merges)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    from = "a",
    to = "b",
    timestamp = time,
    event_subtype = "Current"
  ))
})

test_that("tessi_changed_emails gets all emails changed through a merge", {
  time <- with_tz(now(), Sys.timezone())

  email_stream <- data.table(
    customer_no = 1,
    eaddress_no = c(1, 2, 2),
    timestamp = c(time, time, time + 1),
    primary_ind = c("Y", "Y", "N"),
    inactive = "N",
    # Debouncing means only one row per timestamp per email
    event_subtype = c("Current", "Creation", "current"),
    address = c("a", "b", "b")
  )

  merges <- data.table(
    status = "S",
    kept_id = 1,
    delete_id = -1,
    merge_dt = time + 1
  )

  stub(tessi_changed_emails, "stream_from_audit", email_stream)
  stub(tessi_changed_emails, "read_sql_table", merges)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    from = "b",
    to = "a",
    timestamp = time + 1,
    event_subtype = "Current"
  ))
})

test_that("tessi_changed_emails gets all emails changed through replacement", {
  time <- with_tz(now(), Sys.timezone())

  email_stream <- data.table(
    customer_no = 1,
    eaddress_no = c(1, 1, 2, 2),
    timestamp = time + c(-1, 0, 0, 1),
    primary_ind = c("Y", "N", "Y", "Y"),
    inactive = "N",
    # Debouncing means only one row per timestamp per email
    # Adding in an additional change row to test dealing with multiple changes
    event_subtype = c("Creation", "Current", "Change", "Current"),
    address = c("a", "a", "b", "b")
  )[c(3, 2, 1)]

  stub(tessi_changed_emails, "stream_from_audit", email_stream)
  stub(tessi_changed_emails, "read_sql_table", merges)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    from = "a",
    to = "b",
    timestamp = time,
    event_subtype = "Change"
  ))
})


# p2_update_email --------------------------------------------------------

expect_message_n <- function(n,...) {
  if(n == 1) { expect_message(...) }
  else { expect_message(expect_message_n(n = n-1, ...)) }
}

test_that("p2_update_email updates the email iff it passes all three tests", {
  PUT <- mock(list(status_code = 200))
  stub(p2_update_email, "httr::PUT", PUT)
  p2_query_api <- mock(NULL,NULL,
                       NULL,contact_response,
                       contact_response,NULL,
                       contact_response,NULL)

  stub(p2_update_email, "p2_query_api", p2_query_api)
  contact_response <- list(contacts = data.table(email = "a"),
                  fieldValues = data.table(field = 1, value = "1"))

  # from doesn't exist
  expect_message_n(2,p2_update_email("from","to",12345), "x From email doesn't match from")
  # to matches
  expect_message_n(2,p2_update_email("from","a",12345), "x To email matches a")
  # customer # doesn't match
  expect_message_n(2,p2_update_email("a","to",12345), "x Customer # doesn't match 1")
  # everything passes!
  expect_message_n(3,p2_update_email("a","to",1), "v Doing it!")

  expect_length(mock_args(PUT),1)
})

test_that("p2_update_email updates the email only if dry_run is FALSE", {
  PUT <- mock(list(status_code = 200))
  stub(p2_update_email, "httr::PUT", PUT)
  p2_query_api <- mock(contact_response,NULL,cycle=T)

  stub(p2_update_email, "p2_query_api", p2_query_api)
  contact_response <- list(contacts = data.table(email = "a"),
                           fieldValues = data.table(field = 1, value = "1"))

  expect_message_n(4,p2_update_email("a","to",1, dry_run = TRUE), "i \\(dry run\\)")
  expect_message_n(3,p2_update_email("a","to",1, dry_run = FALSE), "v Doing it!")

  expect_length(mock_args(PUT),1)
})

test_that("p2_update_email sends to api and logs info", {
  PUT <- mock(list(status_code = 200),cycle=T)
  stub(p2_update_email, "httr::PUT", PUT)
  p2_query_api <- mock(contact_response,NULL,cycle=T)

  stub(p2_update_email, "p2_query_api", p2_query_api)
  contact_response <- list(contacts = data.table(email = "a", id = 54321),
                           fieldValues = data.table(field = 1, value = "1"))

  expect_message_n(3,p2_update_email("a","to",1, dry_run = FALSE), "api/3/contacts/54321")
  expect_message_n(3,p2_update_email("a","to",1, dry_run = FALSE), '\\{"email":"to"\\}')

  expect_equal(mock_args(PUT)[[1]][["body"]],list(contact = list(email = "to")))
})

# p2_update_orphans -------------------------------------------------------

test_that("p2_update_orphans passes args on to tessi_changed_emails", {
  tessi_changed_emails <- mock(data.table())

  stub(p2_update_orphans, "tessi_changed_emails", tessi_changed_emails)
  stub(p2_update_orphans, "p2_update_email", NULL)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table())

  time <- now()

  p2_update_orphans(since = time, freshness = Inf)

  expect_equal(mock_args(tessi_changed_emails)[[1]][["since"]],time)
  expect_equal(mock_args(tessi_changed_emails)[[1]][["freshness"]],Inf)

})

test_that("p2_update_orphans runs p2_update_email for each changed email", {
  p2_update_email <- mock(NULL, cycle = T)

  stub(p2_update_orphans, "tessi_changed_emails", data.table(from = letters, to = letters,
                                                             customer_no = 1))
  stub(p2_update_orphans, "p2_update_email", p2_update_email)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table(customer_no = 1,
                                                              merged_customer_no = 1))

  p2_update_orphans()

  expect_length(mock_args(p2_update_email),26)
})

test_that("p2_update_orphans passes merged customer numbers on to p2_update_email", {
  p2_update_email <- mock(NULL, cycle = T)

  stub(p2_update_orphans, "tessi_changed_emails", data.table(from = letters, to = letters,
                                                             customer_no = seq(26)))
  stub(p2_update_orphans, "p2_update_email", p2_update_email)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table(customer_no = seq(2600),
                                                              merged_customer_no = seq(26)))

  p2_update_orphans()

  expect_length(mock_args(p2_update_email),26)
  expect_equal(purrr::map(mock_args(p2_update_email),"customer_no"),
               structure(split(seq(2600),f=seq(26)),names = NULL))
})

test_that("p2_update_orphans passes on dry_run for non-matching emails", {
  p2_update_email <- mock(NULL, cycle = T)

  stub(p2_update_orphans, "tessi_changed_emails", data.table(from = letters, to = letters,
                                                             customer_no = 1))
  stub(p2_update_orphans, "p2_update_email", p2_update_email)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table(customer_no = 1,
                                                              merged_customer_no = 1))

  p2_update_orphans(test_emails = "z")

  expect_length(mock_args(p2_update_email),26)
  expect_equal(purrr::map(mock_args(p2_update_email),"dry_run"),
               as.list(c(rep(T,25),F)))
})
