# TEST: last audited value matches current state of address
aCurrent <- a[, .(group_customer_no, address_no, last_updated_by,
  timestamp = last_update_dt,
  primary_ind, street1, street2, street3, city, state = state_short_desc, country = country_desc, postal_code
)] %>%
  data.table::melt(
    variable.name = "column_updated", value.name = "new_value",
    id.vars = c("group_customer_no", "address_no", "last_updated_by", "timestamp")
  ) %>%
  .[, `:=`(old_value = NA, new_value = trimws(tolower(new_value)))]

aTest <- aAudit[, .SD[.N], by = c("address_no", "column_updated")] %>%
  merge(aCurrent, by = c("address_no", "column_updated"), all.x = TRUE, suffix = c(".a", ".c"))

aTestFail <- aTest[column_updated %in% c("street1", "city", "state", "postal_code", "primary_ind")] %>%
  .[column_updated == "postal_code", c("new_value.a", "new_value.c") := lapply(.SD, substr, 1, 5),
    .SDcols = c("new_value.a", "new_value.c")
  ] %>%
  .[new_value.a != new_value.c & timestamp.a < lubridate::today() - lubridate::ddays(1) & timestamp.c < lubridate::today() - lubridate::ddays(1)]

# Greater than 99.8% of address data matches
testit::assert(nrow(aTestFail) / nrow(aTest) < .002)
# And not counting NCOA, which shouldn't affect localization, we are at 99.9%
testit::assert(nrow(aTestFail[!grepl("NCOA", last_updated_by.c)]) / nrow(aTest) < .001)


# TEST: audited values are internally consistent
testit::assert(aAudit[, .(old_value == lag(new_value) & !is.na(old_value) |
  column_updated == "postal_code" & substr(old_value, 1, 5) == lag(substr(new_value, 1, 5)) |
  column_updated == "street2" |
  column_updated != lag(column_updated) | address_no != lag(address_no) |
  .I == 1)] %>% table() %>%
  {
    .[2] / sum(.)
  } > .999)

# 99.9% of customers have a primary address
testit::assert(nrow(addressStream[, any(primary_ind == "y"), by = "group_customer_no"][V1 == TRUE]) /
  dplyr::n_distinct(addressStream$group_customer_no) > .999)
# 99% of addresses now have a postal code
testit::assert(nrow(addressStream[is.na(postal_code)]) / nrow(addressStream) < .01)
