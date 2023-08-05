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
  expect_gte(length(unique(contributions_matched$cust_memb_no))/memberships[memb_amt>0,.N], .9)
  expect_lte(contributions_matched[coalesce(cust_memb_no,-1)==coalesce(cust_memb_no_real,-1),.N]/contributions_matched[,.N],1)
  expect_lte(contributions_matched[!is.na(cust_memb_no), .N]/contributions_matched[!is.na(cust_memb_no_real), .N], 1)
  expect_lte(length(unique(contributions_matched$cust_memb_no))/memberships[memb_amt>0,.N], 1)
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
