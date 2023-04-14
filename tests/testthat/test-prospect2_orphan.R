withr::local_package("mockery")
withr::local_package("lubridate")

# tessi_changed_emails ----------------------------------------------------
email_stream <- data.table(
  customer_no = 1,
  group_customer_no = 1,
  eaddress_no = 1,
  timestamp = now(),
  primary_ind = "Y",
  inactive = "N",
  event_subtype = c("Current"),
  address = "a",
  last_updated_by = "me")

# test_that("tessi_changed_emails", {
#   stream_from_audit <- mock(readRDS("primary_emails.Rds"))
#   stub(tessi_changed_emails,"stream_from_audit",stream_from_audit)
#   debugonce(tessi_changed_emails)
#   tessi_changed_emails()
#
# })

test_that("tessi_changed_emails passes ... on to stream_from_audit and read_sql_table", {
  stream_from_audit <- mock(email_stream)

  stub(tessi_changed_emails, "stream_from_audit", stream_from_audit)

  tessi_changed_emails(a = "b", c = "d")
  expect_equal(mock_args(stream_from_audit)[[1]][["a"]], "b")
  #expect_equal(mock_args(read_sql_table)[[1]][["c"]], "d")
})

test_that("tessi_changed_emails gets all emails changed at a particular point in time", {
  time <- with_tz(now(), Sys.timezone())

  email_stream <- data.table(
    customer_no = 1,
    group_customer_no = 1,
    eaddress_no = 1,
    timestamp = time + dhours(seq(-1, 1)),
    primary_ind = "Y",
    inactive = "N",
    # Debouncing means only one row per timestamp per email
    event_subtype = c("Creation", "Current", "Changed"),
    address = c("a", "b", "b"),
    last_updated_by = "me"
  )[.N:1]

  stub(tessi_changed_emails, "stream_from_audit", email_stream)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    group_customer_no = 1,
    from = "a",
    to = "b",
    timestamp = time,
    last_updated_by = "me"
  ))
})

test_that("tessi_changed_emails gets all emails changed through a merge", {
  time <- with_tz(now(), Sys.timezone())

  stub(tessi_changed_emails, "tessi_customer_no_map", data.table(customer_no = -1,
                                                                 merged_customer_no = 1))

  email_stream <- data.table(
    customer_no = 1,
    group_customer_no = 1,
    eaddress_no = c(1, 2, 2, 3, 3),
    timestamp = c(time, time, time + 1, time - 1, time + 2),
    primary_ind = c("Y", "Y", "N", "Y", "N"),
    inactive = "N",
    # Debouncing means only one row per timestamp per email
    event_subtype = c("Current", "Creation", "Current", "Creation", "Current"),
    address = c("a", "b", "b", "c", "c"),
    last_updated_by = "me"
  )

  stub(tessi_changed_emails, "stream_from_audit", email_stream)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    group_customer_no = 1,
    from = c("b", "c"),
    to = c("c", "a"),
    timestamp = c(time + 1, time + 2),
    last_updated_by = "me"
  ))
})

test_that("tessi_changed_emails gets all emails changed through replacement", {
  time <- with_tz(now(), Sys.timezone())

  email_stream <- data.table(
    customer_no = 1,
    group_customer_no = 1,
    eaddress_no = c(1, 1, 2, 2),
    timestamp = time + c(-1, 0, 1, 0),
    primary_ind = c("Y", "N", "Y", "Y"),
    inactive = "N",
    # Debouncing means only one row per timestamp per email
    # Adding in an additional change row to test dealing with multiple changes
    event_subtype = c("Creation", "Current", "Change", "Current"),
    address = c("a", "a", "b", "b"),
    last_updated_by = "me"
  )[.N:1]

  stub(tessi_changed_emails, "stream_from_audit", email_stream)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    group_customer_no = 1,
    from = "a",
    to = "b",
    timestamp = time,
    last_updated_by = "me"
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

# p2_orphans --------------------------------------------------------------
tessistream$p2_db <- DBI::dbConnect(RSQLite::SQLite(),":memory:")
withr::defer(p2_db_close())

# all contacts
contacts <- data.table(id=seq(100),email=data.table::CJ(a=letters,b=letters)[1:100,paste(a,b)])
# the first 90 have customer #s
fieldValues <- data.table(id=seq(90)+2000,contact=seq(90),field=1,value=seq(90)+1000)
# two thirds are subscribed to a list
contactLists <- data.table(id=seq(200)+1000,
                           contact=rep(seq(100),2),
                           status=rep(c(1,2),length.out=200))

copy_to(tessistream$p2_db,contacts,"contacts")
copy_to(tessistream$p2_db,fieldValues,"fieldValues")
copy_to(tessistream$p2_db,contactLists,"contactLists")

p2_contacts <-  dplyr::inner_join(contacts,fieldValues,by=c("id"="contact")) %>%
  dplyr::inner_join(contactLists[status==1],by=c("id"="contact")) %>%
  select(id,address=email,customer_no=value) %>%
  distinct %>% dplyr::arrange(id)

test_that("p2_orphans gets all the current subscribers from p2 database",{


  stub(p2_orphans,"read_tessi",data.table(address=NA,
                                          primary_ind=NA,
                                          customer_no=NA))
  stub(p2_orphans,"p2_db_open",NULL)

  expect_mapequal(p2_orphans() %>% setkey(id),p2_contacts)

})


test_that("p2_orphans returns all orphans",{
  stub(p2_orphans,"read_tessi",data.table(address=paste(letters,letters),
                                          primary_ind=c("Y","N"),
                                          customer_no=seq(26)))
  stub(p2_orphans,"p2_db_open",NULL)

  expect_mapequal(p2_orphans() %>% setkey(id),
                  p2_contacts[!address %in% c("a a", "c c")])

})

# p2_orphans_report -------------------------------------------------------

png <- tempfile(fileext=".png")
xlsx <- tempfile(fileext=".xlsx")
html <- tempfile()
tempfile <- mock(png,xlsx,html)

p2_update_email <- mock(T,F,cycle=T)
send_email <- mock()

stub(p2_orphans_report, "p2_orphans", data.table(address=letters,
                                                 customer_no=1,
                                                 id=1))
stub(p2_orphans_report, "tessi_changed_emails", data.table(from=letters,
                                                           to=LETTERS,
                                                           last_updated_by=rep(c("popmulti","sqladmin","me"),length.out=26),
                                                           timestamp = lubridate::ymd_hms("2023-01-01 00:00:00") + ddays(seq(26)),
                                                           customer_no=1,
                                                           group_customer_no=1))
stub(p2_orphans_report, "read_tessi", data.table(customer_no=1,
                                                 group_customer_no=1,
                                                 memb_level="L01",
                                                 expr_dt=now()))
stub(p2_orphans_report, "tempfile", tempfile)
stub(p2_orphans_report, "p2_update_email", p2_update_email)
stub(p2_orphans_report, "send_email", send_email)

p2_orphans_report()

test_that("p2_orphans_report creates a chart and a spreadsheet of orphans analysis",{

  expect_length(png::readPNG(png),480*480*3)
  # test ink converage > 50%
  expect_gte(length(which(png::readPNG(png)<1)),480*480*3/2)
  expect_equal(nrow(openxlsx::read.xlsx(xlsx)),26)

})

test_that("p2_orphans_report sends an email with the spreadsheet and an inline image",{

  expect_match(readLines(html),gsub("\\","\\\\",png,fixed=T))
  expect_equal(mock_args(send_email)[[1]][["attach.files"]],xlsx)

})

test_that("p2_orphans_report determines the updatability of each row",{

  expect_equal(purrr::map_chr(mock_args(p2_update_email),1),letters)
  expect_equal(purrr::map_chr(mock_args(p2_update_email),2),LETTERS)
  expect_equal(purrr::map_lgl(mock_args(p2_update_email),"dry_run"),rep(T,26))
  expect_equal(openxlsx::read.xlsx(xlsx)$can.be.Updated, rep(c(T,F),13))

})
