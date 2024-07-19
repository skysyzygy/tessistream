withr::local_package("mockery")
withr::local_package("checkmate")

# address_clean -----------------------------------------------------------

test_that("address_clean removes tabs and newlines and titlecases", {
  expect_equal(address_clean(c("\tsome  very messy\naddress","an address    with     room          ")),
                             c("Some Very Messy Address","An Address With Room"))
})

test_that("address_clean replaces junk info with NA", {
  expect_equal(address_clean(c("1234 ok st","","no address"),"^$|^no add"),
               c("1234 Ok St",NA,NA))
})


# address_normalize -------------------------------------------------------


test_that("address_normalize combines data from Google, the US Census, libpostal, and Tessitura",{

})

test_that("address_normalize fills in street2 when available (but doesn't duplicate street1)",{

})

test_that("address_normalize adds unit info to street1 when there's no room in street2",{

})

test_that("address_normalize attempts to eliminate issues with duplicated apartment details by using street2",{

})

test_that("address_normalize gets the case of state/country right",{

})

test_that("address_normalize returns only address_cols and address_cols_cleaned",{

})
