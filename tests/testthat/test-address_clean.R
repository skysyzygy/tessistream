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
address_stream <- data.table("street1" = rep("30 Lafayette Ave",4),
                             "street2" = NA,
                             "city" = "Brooklyn",
                             "state" = "NY",
                             "postal_code" = "11217",
                             "country" = NA_character_)
address_parsed <- cbind(address_stream,libpostal.house_number="30",libpostal.road=c(rep("libpostal ave",3),NA),
                           libpostal.city = "brooklyn", libpostal.state="ny", libpostal.postcode = "11217",
                           libpostal.country = NA, libpostal.po_box = NA, libpostal.unit = NA, libpostal.house = NA)
address_geocoded <- cbind(address_stream,
                          query=c("census address_street1","google address_street1",NA,NA),
                          matched_address = c("30 CENSUS AVE, BROOKLYN, NY, 11217", NA,NA,NA),
                          formatted_address = c(NA,rep("30 Google Ave, Brooklyn, NY 11217, USA",3)),
                          geometry.location_type = c(NA,"ROOFTOP","APPROXIMATE","APPROXIMATE"))

test_that("address_normalize combines data from Google, the US Census, libpostal, and Tessitura",{
  address_stream <- copy(address_stream)
  address_parsed <- copy(address_parsed)
  address_geocoded <- copy(address_geocoded)

  stub(address_normalize,"address_parse",address_parsed)
  stub(address_normalize,"address_geocode",address_geocoded)

  normalized <- address_normalize(address_stream)
  expect_equal(normalized$street1_cleaned, c("30 Census Ave", "30 Google Ave", "30 Libpostal Ave", "30 Lafayette Ave"))

})

test_that("address_normalize fills in street2 when available (but doesn't duplicate street1)",{
  address_stream <- copy(address_stream)
  address_parsed <- copy(address_parsed)
  address_geocoded <- copy(address_geocoded)

  address_parsed$libpostal.unit <- c("Apt 1",NA, NA, NA)
  address_parsed$libpostal.po_box <- c(NA, "P.O. Box 12345", NA, "30 Lafayette Ave")
  address_parsed$libpostal.house <- c(NA, NA, "Brooklyn Academy Of Music", NA)

  stub(address_normalize,"address_parse",address_parsed)
  stub(address_normalize,"address_geocode",address_geocoded)

  normalized <- address_normalize(address_stream)
  expect_equal(normalized$street2_cleaned, c("Apt 1", "P.O. Box 12345", "Brooklyn Academy Of Music", NA))

})

test_that("address_normalize adds unit info to street1 when there's no room in street2",{
  address_stream <- copy(address_stream)
  address_parsed <- copy(address_parsed)
  address_geocoded <- copy(address_geocoded)

  address_parsed$libpostal.unit <- c(rep("Apt 1",2),NA,NA)
  address_parsed$libpostal.po_box <- c(NA, "P.O. Box 12345", NA, "30 Lafayette Ave")
  address_parsed$libpostal.house <- c(NA, NA, "Brooklyn Academy Of Music", NA)

  stub(address_normalize,"address_parse",address_parsed)
  stub(address_normalize,"address_geocode",address_geocoded)

  normalized <- address_normalize(address_stream)
  expect_equal(normalized$street2_cleaned, c("Apt 1", "P.O. Box 12345", "Brooklyn Academy Of Music", NA))
  expect_equal(normalized$street1_cleaned, c("30 Census Ave", "30 Google Ave, Apt 1", "30 Libpostal Ave", "30 Lafayette Ave"))
})

test_that("address_normalize attempts to eliminate issues with duplicated apartment details by using street2",{
  address_stream <- copy(address_stream)
  address_parsed <- copy(address_parsed)
  address_geocoded <- copy(address_geocoded)

  address_parsed$street2 <- c("Apt. 1", "# B2", "PH", "Bsmt")
  address_parsed$libpostal.unit <- paste(address_parsed$street2,address_parsed$street2)
  address_parsed$street1 <- paste(address_parsed$street1,address_parsed$street2)

  stub(address_normalize,"address_parse",address_parsed)
  stub(address_normalize,"address_geocode",address_geocoded)

  normalized <- address_normalize(address_stream)
  expect_equal(normalized$street2_cleaned, c("Apt. 1", "# B2", "Ph", "Bsmt"))
  expect_equal(normalized$street1_cleaned, c("30 Census Ave", "30 Google Ave", "30 Libpostal Ave", "30 Lafayette Ave Bsmt"))
})

test_that("address_normalize gets the case of state/country right",{
  address_stream <- copy(address_stream)
  address_parsed <- copy(address_parsed)
  address_geocoded <- copy(address_geocoded)

  stub(address_normalize,"address_parse",address_parsed)
  stub(address_normalize,"address_geocode",address_geocoded)

  normalized <- address_normalize(address_stream)
  expect_equal(normalized$state_cleaned, rep("NY",4))
  expect_equal(normalized$country_cleaned, c(rep("USA",2),NA,NA))
})

test_that("address_normalize returns only address_cols and address_cols_cleaned",{
  address_stream <- copy(address_stream)
  address_parsed <- copy(address_parsed)
  address_geocoded <- copy(address_geocoded)

  stub(address_normalize,"address_parse",address_parsed)
  stub(address_normalize,"address_geocode",address_geocoded)

  normalized <- address_normalize(address_stream)
  expect_names(names(normalized), permutation.of=c(address_cols,paste0(address_cols,"_cleaned")))

})
