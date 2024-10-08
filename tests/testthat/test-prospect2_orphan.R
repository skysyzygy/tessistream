withr::local_package("mockery")
withr::local_package("lubridate")

# tessi_changed_emails ----------------------------------------------------

test_that("tessi_changed_emails passes ... on to stream_from_audit and read_sql_table", {
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
    last_updated_by = c("me", "you", "admin")
  )[.N:1]

  stub(tessi_changed_emails, "stream_from_audit", email_stream)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    group_customer_no = 1,
    from = "a",
    to = "b",
    timestamp = time,
    last_updated_by = "you"
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
    last_updated_by = c("me", "you", "them", "her", "him")
  )

  stub(tessi_changed_emails, "stream_from_audit", email_stream)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    group_customer_no = 1,
    from = c("b", "c"),
    to = c("c", "a"),
    timestamp = c(time + 1, time + 2),
    last_updated_by = c("them", "him")
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
    last_updated_by = c("me", "you", "admin", "her")
  )[.N:1]

  stub(tessi_changed_emails, "stream_from_audit", email_stream)

  expect_mapequal(tessi_changed_emails(), data.table(
    customer_no = 1,
    group_customer_no = 1,
    from = "a",
    to = "b",
    timestamp = time,
    last_updated_by = "you"
  ))
})


# p2_resolve_orphan --------------------------------------------------------

expect_message_n <- function(n,...) {
  if(n == 1) { expect_message(...) }
  else { expect_message(expect_message_n(n = n-1, ...)) }
}

test_that("p2_resolve_orphan updates the email iff it passes three tests", {
  p2_update_email <- mock()
  stub(p2_resolve_orphan, "p2_update_email", p2_update_email)
  p2_query_api <- mock(list(meta = NULL),list(meta = NULL),
                       list(meta = NULL),contact_response,
                       contact_response,list(meta = NULL),
                       contact_response,list(meta = NULL))

  stub(p2_resolve_orphan, "p2_query_api", p2_query_api)
  contact_response <- list(contacts = data.table(email = "a", id = "99"),
                  fieldValues = data.table(field = "1", value = "1"))

  # from doesn't exist
  expect_message_n(2,p2_resolve_orphan("from","to",12345), "x From email is in P2 : from")
  # to matches
  expect_message_n(2,p2_resolve_orphan("from","a",12345), "x To email is not in P2 : a")
  # customer # doesn't match
  expect_message_n(2,p2_resolve_orphan("a","to",12345), "x Customer # matches : 12345")
  # everything passes!
  expect_message_n(2,p2_resolve_orphan("a","to",c(1,12345)))
  expect_length(mock_args(p2_update_email),1)
})

test_that("p2_resolve_orphan adds a tag iff it passes two tests", {
  p2_add_tag <- mock()
  stub(p2_resolve_orphan, "p2_add_tag", p2_add_tag)
  p2_query_api <- mock(contact_response,contact_response)

  stub(p2_resolve_orphan, "p2_query_api", p2_query_api)
  contact_response <- list(contacts = data.table(email = "a", id = "99"),
                           fieldValues = data.table(field = "1", value = "1"))

  expect_message_n(2,p2_resolve_orphan("a","b",1))
  expect_length(mock_args(p2_add_tag),1)
})

test_that("p2_resolve_orphan does nothing if `from` or `customer_no` is empty, and gracefully handles empty `to`", {
  p2_update_email <- mock()
  p2_add_tag <- mock()
  stub(p2_resolve_orphan, "p2_update_email", p2_update_email)
  stub(p2_resolve_orphan, "p2_add_tag", p2_add_tag)
  p2_query_api <- mock(contact_response, cycle = T)

  stub(p2_resolve_orphan, "p2_query_api", p2_query_api)
  contact_response <- list(contacts = data.table(email = "a", id = "99"),
                           fieldValues = data.table(field = "1", value = "1"))

  # from is empty
  expect_message_n(2,p2_resolve_orphan("","to",1))
  expect_message_n(2,p2_resolve_orphan(NULL,"to",1))
  expect_message_n(2,p2_resolve_orphan(NA,"to",1))
  expect_length(mock_args(p2_update_email),0)
  expect_length(mock_args(p2_add_tag),0)

  # customer_no is empty
  expect_message_n(2,p2_resolve_orphan("from","a",""))
  expect_message_n(2,p2_resolve_orphan("from","a",NULL))
  expect_message_n(2,p2_resolve_orphan("from","a",NA))
  expect_length(mock_args(p2_update_email),0)
  expect_length(mock_args(p2_add_tag),0)

  # to is empty
  expect_message_n(2,p2_resolve_orphan("a","",1))
  expect_message_n(2,p2_resolve_orphan("a",NULL,1))
  expect_message_n(2,p2_resolve_orphan("a",NA,1))
  expect_length(mock_args(p2_update_email),0)
  expect_length(mock_args(p2_add_tag),3)

})

# p2_update_email ---------------------------------------------------------

test_that("p2_update_email calls p2_execute_api", {
  p2_execute_api <- mock(TRUE)
  stub(p2_update_email, "p2_execute_api", p2_execute_api)

  expect_true(p2_update_email(12345, "test@test.com", dry_run = TRUE))

  p2_object = jsonlite::fromJSON('{"contact": {"email": "test@test.com"}}')

  expect_length(mock_args(p2_execute_api),1)
  expect_match(mock_args(p2_execute_api)[[1]][["url"]],"api/3/contacts/12345")
  expect_equal(mock_args(p2_execute_api)[[1]][["object"]],p2_object)
  expect_equal(mock_args(p2_execute_api)[[1]][["method"]],"PUT")
  expect_equal(mock_args(p2_execute_api)[[1]][["dry_run"]], TRUE)
})

test_that("p2_update_email passes on the results of p2_execute_api", {
  p2_execute_api <- mock(FALSE)
  stub(p2_update_email, "p2_execute_api", p2_execute_api)

  expect_false(p2_update_email(12345, "test@test.com", dry_run = TRUE))
})


# p2_add_tag --------------------------------------------------------------

tags <- readRDS(rprojroot::find_testthat_root_file("tags.Rds"))

test_that("p2_add_tag gets the tag id", {
  tags$tags <- filter(tags$tags, tag == "one")
  p2_query_api <- mock(tags)
  stub(p2_add_tag, "p2_query_api", p2_query_api)

  p2_execute_api <- mock(TRUE)
  stub(p2_add_tag, "p2_execute_api", p2_execute_api)

  p2_add_tag(12345, "one", dry_run = TRUE)

  expect_length(mock_args(p2_query_api),1)
  expect_match(mock_args(p2_query_api)[[1]][[1]],"api/3/tags\\?filters%5Bsearch%5D%5Beq%5D=one$")
})

test_that("p2_add_tag calls p2_execute_api", {
  tags$tags <- filter(tags$tags, tag == "two")
  p2_query_api <- mock(tags)
  stub(p2_add_tag, "p2_query_api", p2_query_api)

  p2_execute_api <- mock(TRUE)
  stub(p2_add_tag, "p2_execute_api", p2_execute_api)

  p2_add_tag(12345, "two", dry_run = TRUE)

  p2_object = jsonlite::fromJSON('{"contactTag": {"contact": 12345, "tag": 2}}')

  expect_length(mock_args(p2_execute_api),1)
  expect_match(mock_args(p2_execute_api)[[1]][["url"]],"api/3/contactTags$")
  expect_equal(mock_args(p2_execute_api)[[1]][["object"]],p2_object)
  expect_equal(mock_args(p2_execute_api)[[1]][["dry_run"]], TRUE)
})

# p2_update_orphans -------------------------------------------------------

test_that("p2_update_orphans passes args on to tessi_changed_emails", {
  tessi_changed_emails <- mock(data.table())

  stub(p2_update_orphans, "tessi_changed_emails", tessi_changed_emails)
  stub(p2_update_orphans, "p2_resolve_orphan", NULL)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table())

  time <- now()

  p2_update_orphans(since = time, freshness = Inf)

  expect_equal(mock_args(tessi_changed_emails)[[1]][["since"]],time)
  expect_equal(mock_args(tessi_changed_emails)[[1]][["freshness"]],Inf)

})

test_that("p2_update_orphans runs p2_resolve_orphan for each changed email", {
  p2_resolve_orphan <- mock(NULL, cycle = T)

  stub(p2_update_orphans, "tessi_changed_emails", data.table(from = letters, to = letters,
                                                             customer_no = 1))
  stub(p2_update_orphans, "p2_resolve_orphan", p2_resolve_orphan)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table(customer_no = 1,
                                                              merged_customer_no = 1))

  p2_update_orphans()

  expect_length(mock_args(p2_resolve_orphan),26)
})

test_that("p2_update_orphans passes merged customer numbers on to p2_resolve_orphan", {
  p2_resolve_orphan <- mock(NULL, cycle = T)

  stub(p2_update_orphans, "tessi_changed_emails", data.table(from = letters, to = letters,
                                                             customer_no = seq(26)))
  stub(p2_update_orphans, "p2_resolve_orphan", p2_resolve_orphan)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table(customer_no = seq(2600),
                                                              merged_customer_no = seq(26)))

  p2_update_orphans()

  expect_length(mock_args(p2_resolve_orphan),26)
  expect_equal(purrr::map(mock_args(p2_resolve_orphan),"customer_no"),
               structure(split(seq(2600),f=seq(26)),names = NULL))
})

test_that("p2_update_orphans passes on dry_run for non-matching emails", {
  p2_resolve_orphan <- mock(NULL, cycle = T)

  stub(p2_update_orphans, "tessi_changed_emails", data.table(from = letters, to = letters,
                                                             customer_no = 1))
  stub(p2_update_orphans, "p2_resolve_orphan", p2_resolve_orphan)
  stub(p2_update_orphans, "tessi_customer_no_map", data.table(customer_no = 1,
                                                              merged_customer_no = 1))

  p2_update_orphans(test_emails = "z")

  expect_length(mock_args(p2_resolve_orphan),26)
  expect_equal(purrr::map(mock_args(p2_resolve_orphan),"dry_run"),
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
                           status=rep(c(1,1,2),length.out=200))

contacts[,(colnames(contacts)) := lapply(.SD,as.character)]
fieldValues[,(colnames(fieldValues)) := lapply(.SD,as.character)]
contactLists[,(colnames(contactLists)) := lapply(.SD,as.character)]

copy_to(tessistream$p2_db,contacts,"contacts")
copy_to(tessistream$p2_db,fieldValues,"fieldValues")
copy_to(tessistream$p2_db,contactLists,"contactLists")

stub(p2_orphans, "tessi_customer_no_map", data.table(customer_no = seq(100) + 1000,
                                                     group_customer_no = seq(100) %% 10))

p2_contacts <-  dplyr::inner_join(contacts,fieldValues,by=c("id"="contact")) %>%
  dplyr::inner_join(contactLists[status==1],by=c("id"="contact")) %>%
  transmute(id = as.integer(id), address=email, customer_no = as.integer(value),
            group_customer_no = as.integer(id) %% 10) %>%
  distinct %>% dplyr::arrange(id)

test_that("p2_orphans gets all the current subscribers from p2 database",{


  stub(p2_orphans,"read_tessi",data.table(address=NA,
                                          primary_ind=NA,
                                          customer_no=NA,
                                          group_customer_no=NA))
  stub(p2_orphans,"p2_db_open",NULL)

  expect_mapequal(p2_orphans() %>% setkey(id),p2_contacts)

})


test_that("p2_orphans returns all orphans",{
  emails <- contacts[1:26,.(address=email,
                            primary_ind=c("Y","N"),
                            customer_no=as.integer(id) + 1000L,
                            group_customer_no = as.integer(id) %% 10)]

  stub(p2_orphans,"read_tessi",emails)
  stub(p2_orphans,"p2_db_open",NULL)

  expect_mapequal(p2_orphans() %>% setkey(id),
                  p2_contacts[!address %in% emails[primary_ind=="Y", address]])

  # matching emails but mismatching group_customer_nos are orphans

  emails[1,group_customer_no:=2]

  expect_mapequal(p2_orphans() %>% setkey(id),
                  p2_contacts[!address %in% emails[primary_ind=="Y", address][-1]])


})
