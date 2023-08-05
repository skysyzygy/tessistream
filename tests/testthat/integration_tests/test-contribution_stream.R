# End to end testing for contributions_stream
withr::local_envvar(R_CONFIG_FILE="")

# contribution_membership_match -------------------------------------------
# Load data from Tessi
contributions <- read_tessi("contributions") %>%
  # Needed because the BI table filters out some contribution references
  left_join(read_sql_table("TX_CONT_MEMB"),
            by = c("ref_no" = "cont_ref_no"), all.x = T, suffix = c(".old","")) %>%
  left_join(read_tessi("creditees", select = c("ref_no", "creditee_no")),
            by = "ref_no", all.x = T, suffix = c("",".creditee")) %>%
  collect %>% setDT
# Add creditee info
contributions[!is.na(group_creditee_no),`:=`(group_customer_no=group_creditee_no, customer_no=creditee_no)]

campaigns <- read_tessi("campaigns", select = c("campaign_no", "fyear", "category_desc")) %>% collect %>% setDT
levels <- read_sql_table("T_MEMB_LEVEL")
memberships <- read_tessi("memberships") %>% filter(!current_status_desc %in% c("Cancelled","Deactivated")) %>% collect %>% setDT
memberships <- merge(memberships,campaigns,by="campaign_no",suffixes = c("",".campaign")) %>%
  merge(levels, by = c("memb_level_no","memb_org_no"), suffixes = c("",".level")) %>%
  setnames("category_desc","campaign_category_desc")

contributions_matched <- contribution_membership_match(contributions, memberships)

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
