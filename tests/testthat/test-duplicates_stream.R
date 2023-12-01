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

# duplicates_exact_match -------------------------------------------------------

test_that("duplicates_exact_match returns all exact matches", {
  data <- data.table(customer_no = seq(1000),
                     feature1 = sample(letters,1000,T),
                     feature2 = sample(letters,1000,T))

  dupes1 <- duplicates_exact_match(data,"feature1")

  expect_equal(data[dupes1,feature1,on="customer_no"],
               data[dupes1,feature1,on=c("customer_no" = "i.customer_no")])

  a <- data[feature1=="a",customer_no]

  dupes1test = data.table(customer_no = a[-1],
                          i.customer_no = a[-length(a)],
                          feature1 = "a")
  expect_equal(dupes1[dupes1test, on=c("customer_no","i.customer_no","feature1")],
               dupes1test)

  dupes2 <- duplicates_exact_match(data,c("feature1","feature2"))

  expect_equal(data[dupes2,feature1,on="customer_no"],
               data[dupes2,feature1,on=c("customer_no" = "i.customer_no")])
  expect_equal(data[dupes2,feature2,on="customer_no"],
               data[dupes2,feature2,on=c("customer_no" = "i.customer_no")])

})


# duplicates_suppress_related ---------------------------------------------

test_that("duplicates_suppress_related suppresses matches by household", {
  stub(duplicates_suppress_related, "tessi_customer_no_map",
       data.table(customer_no = seq(52),
                  group_customer_no = rep(seq(26), 2)))
  stub(duplicates_suppress_related, "read_sql_table",
       data.table(customer_no = integer(),
                  associated_customer_no = integer()))

  # these aren't in the same household
  data <- data.table(customer_no = seq(52),
                     i.customer_no = seq(52)+1)
  # so they come through without change
  expect_equal(duplicates_suppress_related(data),data)

  # add some that are...
  data <- rbind(data,data.table(customer_no = seq(26),
                                i.customer_no = seq(26) + 26))
  # and make sure they're suppressed
  expect_equal(duplicates_suppress_related(data),data[1:52])

})

# duplicates_append_data --------------------------------------------------

test_that("duplicates_append_data makes choices for keep/delete and returns this data", {
  withr::local_package("lubridate")
  n <- 1000
  memberships <- data.table(customer_no = seq(n),
                            cur_record_ind = "Y",
                            memb_amt = 1,
                            memb_level = sample(c("Member",NA),n,T))
  logins <- data.table(customer_no = sample(seq(n),n/3),
                       inactive = "N",
                        last_login_dt = runif(n/3,
                                              ymd("1900-01-01"),
                                              ymd("2100-01-01")))
  customers <- data.table(customer_no = seq(n),
                          inactive_desc = "Active",
                       last_update_dt = runif(n,
                                             ymd("1900-01-01"),
                                             ymd("2100-01-01")),
                       last_activity_dt = runif(n,
                                              ymd("1900-01-01"),
                                              ymd("2100-01-01")))
  customers[sample(n,n/3),last_activity_dt := NA]

  stub(duplicates_append_data, "read_tessi", mock(memberships, logins, customers))

  data <- data.table(customer_no = seq(1000),
                     i.customer_no = seq(1000)+1)

  data <- duplicates_append_data(data)

  reasons <- sort(c("Current membership",
                    "Last activity date",
                    "Last login date",
                    "Last update date"))
  # All reasons are represented
  expect_equal(data[,sort(unique(keep_reason))],reasons)

  current_memberships <- memberships[!is.na(memb_level),customer_no]

  # if customer has membership then keep them
  customer_has_membership <- data[customer_no %in% current_memberships &
                                    !i.customer_no %in% current_memberships]
  expect_equal(customer_has_membership[,keep_customer_no],
               customer_has_membership[,customer_no])

  # if i.customer has membership then keep them
  i.customer_has_membership <- data[!customer_no %in% current_memberships &
                                    i.customer_no %in% current_memberships]
  expect_equal(i.customer_has_membership[,keep_customer_no],
               i.customer_has_membership[,i.customer_no])

  # set the reasons appropriately
  expect_equal(customer_has_membership[,unique(keep_reason)],"Current membership")
  expect_equal(i.customer_has_membership[,unique(keep_reason)],"Current membership")

  # but don't set when undecideable
  both_have_membership <- data[customer_no %in% current_memberships &
                               i.customer_no %in% current_memberships]

  expect_equal(both_have_membership[, sort(unique(keep_reason))],
               setdiff(reasons, "Current membership"))

  neither_have_membership <- data[!customer_no %in% current_memberships &
                                  !i.customer_no %in% current_memberships]

  expect_equal(neither_have_membership[, sort(unique(keep_reason))],
               setdiff(reasons, "Current membership"))


})

