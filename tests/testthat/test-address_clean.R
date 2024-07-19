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


