withr::local_package("checkmate")
withr::local_package("mockery")
tessilake:::local_cache_dirs()

# contribution_membership_match -------------------------------------------
contributions <- readRDS(rprojroot::find_testthat_root_file("contribution_stream-contributions.Rds"))
memberships <-  readRDS(rprojroot::find_testthat_root_file("contribution_stream-memberships.Rds"))
contributions_matched <- contribution_membership_match(contributions, memberships)

test_that("contribution_membership_match returns a table with the same ref_no as the contributions dataset and cust_memb_no as the memberships dataset", {
  expect_equal(contributions_matched$ref_no,contributions$ref_no)
  expect_true(all(contributions_matched$cust_memb_no %in% c(contributions$cust_memb_no,
                                                            memberships$cust_memb_no)))
})

test_that("contribution_membership_match identifies matches for 90% of memberships and membership contributions", {
  expect_gte(contributions_matched[coalesce(cust_memb_no,-1)==coalesce(cust_memb_no_real,-1),.N]/contributions_matched[,.N],.9)
  expect_gte(contributions_matched[!is.na(cust_memb_no), .N]/contributions_matched[!is.na(cust_memb_no_real), .N], .9)
  expect_gte(memberships[cust_memb_no %in% contributions_matched$cust_memb_no & memb_amt>0,.N]/memberships[memb_amt>0,.N], .9)
  expect_lte(contributions_matched[coalesce(cust_memb_no,-1)==coalesce(cust_memb_no_real,-1),.N]/contributions_matched[,.N],1)
  expect_lte(contributions_matched[!is.na(cust_memb_no), .N]/contributions_matched[!is.na(cust_memb_no_real), .N], 1)
  expect_lte(memberships[cust_memb_no %in% contributions_matched$cust_memb_no & memb_amt>0,.N]/memberships[memb_amt>0,.N], 1)
})

test_that("contribution_membership_match does not mismatch customers", {
  contributions_matched <- contributions_matched[!contributions[!is.na(cust_memb_no)],on="ref_no"]
  expect_equal(memberships[contributions_matched,sum(group_customer_no!=i.group_customer_no, na.rm=T),on="cust_memb_no"],0)
})


test_that("contribution_membership_match does not overmatch any membership", {
  contributions_matched <- contributions_matched[!contributions[!is.na(cust_memb_no)],on="ref_no"]
  membership_contributions <- contributions_matched[,.(cont_amt = sum(cont_amt)),by="cust_memb_no"]
  expect_equal(memberships[membership_contributions,on="cust_memb_no"][cont_amt > pmax(end_amt,memb_amt+AVC_amt,recog_amt),.N],0)
})

# contribution_stream -----------------------------------------------------

stub(contribution_stream, "contribution_stream_load", list(contributions = contributions, memberships = memberships))
stub(contribution_stream, "contribution_stream_match", contributions_matched)
stub(contribution_stream, "write_cache", NULL)
.contribution_stream <- contribution_stream

contribution_stream <- .contribution_stream()

test_that("contribution_stream has a row for each contribution and references (almost) all memberships", {
  expect_equal(contribution_stream[, .N, by="ref_no"][N>1,.N], 0)
  expect_equal(contribution_stream[, sort(ref_no)], contributions[cont_amt>0, sort(ref_no)])
  expect_equal(contribution_stream[, sum(cont_amt), by = list(year = year(timestamp))] %>% setkeyv("year"),
               contributions[, sum(cont_amt), by = list(year = year(cont_dt))] %>% setkeyv("year"))

  expect_gte(memberships[cust_memb_no %in% contribution_stream$cust_memb_no,.N,on="cust_memb_no"]/
             memberships[,.N], .9)
  expect_lte(memberships[cust_memb_no %in% contribution_stream$cust_memb_no,.N,on="cust_memb_no"]/
               memberships[,.N], 1)

})

test_that("contribution_stream has all of the advertised columns", {
  expect_names(colnames(contribution_stream),
               must.include = c("group_customer_no","timestamp","event_type","event_subtype",
                                "event_subtype2","cont_amt","campaign_no","ref_no","cust_memb_no"))

  expect_names(colnames(contribution_stream),
               must.include = c("contribution_timestamp_min", "contribution_timestamp_max",
                                "contribution_timestamp_last", "contribution_amt", "contribution_max"))

  expect_names(colnames(contribution_stream),
               must.include = paste0("contribution_",unique(contribution_stream$event_subtype),"_count"))

})

test_that("contribution_stream calculates all of the numeric and date columns according to their names", {
  expect_equal(contribution_stream$contribution_timestamp_min, contribution_stream[, rep(min(timestamp),.N), by = "group_customer_no"]$V1)
  expect_equal(contribution_stream$contribution_timestamp_max, contribution_stream$timestamp)
  expect_equal(contribution_stream$contribution_timestamp_last, contribution_stream[, timestamp[lag(cumsum(!duplicated(timestamp)))],
                                                                                                by = "group_customer_no"]$V1)
  expect_equal(contribution_stream$contribution_amt, contribution_stream[, cumsum(cont_amt), by = "group_customer_no"]$V1)
  expect_equal(contribution_stream$contribution_max, contribution_stream[, cummax(cont_amt), by = "group_customer_no"]$V1)
  expect_equal(contribution_stream$`contribution_Campaign A_count`, contribution_stream[, cumsum(event_subtype == "Campaign A"),
                                                                                        by = "group_customer_no"]$V1)
})

test_that("contribution_stream sets event_subtype for all contributions, based on the input logic", {

  # simple case, reference data already in the table
  contribution_stream <- .contribution_stream(event_subtypes = list(cont_amt>1000 ~ "Big",
                                                                    cont_amt>100 ~ "Small",
                                                                    TRUE ~ "Tiny"))

  expect_equal(sort(contribution_stream[,unique(event_subtype)]),c("Big","Small","Tiny"))

  expect_equal(contributions[cont_amt>1000,sort(ref_no)],
               contribution_stream[event_subtype == "Big",sort(ref_no)])
  expect_equal(contributions[cont_amt>100 & cont_amt<=1000,sort(ref_no)],
               contribution_stream[event_subtype == "Small",sort(ref_no)])
  expect_equal(contributions[cont_amt > 0 & cont_amt<=100,sort(ref_no)],
               contribution_stream[event_subtype == "Tiny",sort(ref_no)])

  # more complicated, reference data from outside by baking it into the expression
  contribution_stream <- eval(rlang::expr(.contribution_stream(event_subtypes = list(cust_memb_no %in% !!memberships[memb_amt > 1000,cust_memb_no] ~ "Big",
                                                                                     cust_memb_no %in% !!memberships[memb_amt > 0,cust_memb_no] ~ "Small",
                                                                                     TRUE ~ "Nothing"))))

  expect_equal(sort(contribution_stream[,unique(event_subtype)]),c("Big","Nothing","Small"))

  expect_equal(contributions_matched[cust_memb_no %in% memberships[memb_amt>1000,cust_memb_no],sort(ref_no)],
               contribution_stream[event_subtype == "Big",sort(ref_no)])
  expect_equal(contributions_matched[cust_memb_no %in% memberships[memb_amt>0 & memb_amt<=1000,cust_memb_no],sort(ref_no)],
               contribution_stream[event_subtype == "Small",sort(ref_no)])
  expect_equal(contributions_matched[cust_memb_no %in% c(memberships[memb_amt==0,cust_memb_no],NA) & cont_amt>0,sort(ref_no)],
               contribution_stream[event_subtype == "Nothing",sort(ref_no)])

})

test_that("contribution_stream sets event_subtype2 for all contributions and all match the expected values", {
  #' *  event_subtype : Membership|Gala|Other
  contributions <- copy(contributions)[cont_amt>0] %>% setkey(group_customer_no,cont_dt)
  contributions_matched <- copy(contributions_matched)[cont_amt>0] %>% setkey(group_customer_no,cont_dt)

  expect_true(!any(is.na(contribution_stream$event_subtype2)))

  expect_equal(contribution_stream[event_subtype2 == "New", ref_no],
               contributions[, ref_no[1], by=c("group_customer_no","campaign_category_desc")]$V1)

  upgrades_test <- contributions_matched[,.(test = !is.na(cust_memb_no) & duplicated(cust_memb_no) |
                                                    is.na(cust_memb_no) & duplicated(campaign_no),
                                            ref_no),
                                         by = c("group_customer_no","campaign_category_desc")][test == T]

  expect_equal(contribution_stream[event_subtype2 == "Upgrade",sort(ref_no)],
               upgrades_test[,sort(ref_no)])

  expect_equal(contribution_stream[event_subtype2 == "Renew", sort(ref_no)],
               contributions[,.(ref_no,test = cont_dt - lag(cont_dt) <= ddays(365)),
                             by=c("group_customer_no","campaign_category_desc")] %>%
                 .[!ref_no %in% upgrades_test$ref_no] %>%
                 .[test == T, sort(ref_no)])

  expect_equal(contribution_stream[event_subtype2 == "Reinstate", sort(ref_no)],
               contributions[,.(ref_no,test = cont_dt - lag(cont_dt) > ddays(365)),
                             by=c("group_customer_no","campaign_category_desc")] %>%
                 .[!ref_no %in% upgrades_test$ref_no] %>%
                 .[test == T, sort(ref_no)])

})

test_that("contribution_stream doesn't leak data through contributionTimestampLast", {
  expect_equal(contribution_stream[contribution_timestamp_last==contribution_timestamp_max |
                                     contribution_timestamp_last==timestamp,.N], 0)
})

