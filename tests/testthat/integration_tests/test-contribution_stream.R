# End to end testing for contributions_stream
withr::local_envvar(R_CONFIG_FILE="")

# contribution_membership_match -------------------------------------------

data <- contribution_stream_load()
contributions <- data$contributions
memberships <- data$memberships

contributions_matched <- contribution_membership_match(data$contributions, data$memberships)

test_that("contribution_membership_match returns a table with the same ref_no as the contributions dataset and cust_memb_no as the memberships dataset", {
  expect_equal(contributions_matched$ref_no,contributions$ref_no)
  expect_true(all(contributions_matched$cust_memb_no %in% c(contributions$cust_memb_no,
                                                            memberships$cust_memb_no)))
})

test_that("contribution_membership_match identifies matches for 99.9% of memberships and membership contributions", {
  membership_contributions <- contributions_matched[grepl("(?<!Non-)Membership|Major", campaign_category_desc, perl = T) & cont_amt>0]

  expect_gte(membership_contributions[!is.na(cust_memb_no), .N]/membership_contributions[, .N], .999)
  expect_gte(memberships[cust_memb_no %in% contributions_matched$cust_memb_no & memb_amt>0,.N]/memberships[memb_amt>0,.N], .99)
  expect_lte(membership_contributions[!is.na(cust_memb_no), .N]/membership_contributions[, .N], 1)
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

# TEST: all contributions made it through
testit::assert(
  full_join(
    contribution_stream %>% group_by(year=year(timestamp)) %>% summarise(cont_amt=sum(cont_amt)),
    c %>% group_by(year=year(cont_dt)) %>% summarise(cont_amt=sum(cont_amt)),
    by="year") %>%
    filter(cont_amt.x!=cont_amt.y | is.na(cont_amt.x) | is.na(cont_amt.y)) %>% nrow == 0
)

# TEST: NAs are less than .1% of each column (except for columns where we expect it...)
testit::assert(all(sapply(contribution_stream[,-c("cust_memb_no","contributionTimestampLast")],function(.){sum(is.na(.))<.001*length(.)})))

# TEST: one and only one new membership per customer in 99.9% of cases
testit::assert(contribution_stream[event_subtype %in% c("Member","Patron","Major Gifts"),
                                   sum(event_subtype2=="New"),by="group_customer_no"][V1>1,.N] <
                 .002*contribution_stream[event_subtype %in% c("Member","Patron","Major Gifts"),.N,by="group_customer_no"][,.N])

# TEST: renewal rate is in a sensible range
testit::assert(
  between(contribution_stream[event_subtype2 %in% c("Renew","Reinstate","Upgrade"),.N]/
            contribution_stream[,.N],
          .7,.9))

# TEST: no data leak in contributionTimestampLast
testit::assert(contribution_stream[contributionTimestampLast==contributionTimestampMax | contributionTimestampLast==timestamp,.N]==0)
