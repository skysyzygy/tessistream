withr::local_package("checkmate")
withr::local_package("mockery")

if(!file.exists(rprojroot::find_testthat_root_file("census_data.sqlite")))
  address_census_prepare_fixtures()

# census_variables --------------------------------------------------------

census_variables <- census_variables()

test_that("census_variables identifies variables for each year and type", {
  years <- c(2000,seq(2009,2021))
  types <- c("race","sex_and_age","income")

  expect_named(census_variables,c("year","type","variable","dataset","label","age","sex","race","measure","concept"),
               ignore.order = TRUE)

  expect_false(census_variables[expand.grid(year=years,type=types),any(is.na(label)),on=c("year","type")])
})

test_that("census_variables identifies a sane number of variables for each acs year and type", {
  variables <- census_variables[!dataset %in% c("sf1","sf3")]
  years <- variables[,unique(year)]
  expect_equal(variables[type == "income",.N,by="year"]$N,rep(2,length(years)))
  expect_equal(variables[type == "race",.N,by="year"]$N,rep(22,length(years)))
  expect_equal(variables[type == "sex_and_age" & is.na(age),.N,by="year"]$N,rep(2,length(years)))
  expect_equal(variables[type == "sex_and_age" & !is.na(age),.N,by="year"]$N,rep(13,length(years)))
})

test_that("census_variables identifies a sane number of variables for each decennial year and type", {
  variables <- census_variables[dataset %in% c("sf1","sf3")]
  years <- variables[,unique(year)]
  expect_equal(variables[type == "income",.N,by="year"]$N,1)
  expect_equal(variables[type == "race",.N,by="year"]$N,rep(7,length(years)))
  expect_equal(variables[type == "sex_and_age" & is.na(age),.N,by="year"]$N,rep(2,length(years)))
  expect_equal(variables[type == "sex_and_age" & !is.na(age),.N,by="year"]$N,rep(200,length(years)))
})

test_that("census_variables categorizes each variable", {
  expect_false(census_variables[type == "sex_and_age",any(is.na(age) & is.na(sex))])
  expect_false(census_variables[type == "race",any(is.na(race))])
  expect_false(census_variables[type == "income",any(is.na(measure))])
})


# census_get_data ---------------------------------------------------------

test_that("census_get_data queries get_acs or get_decennial, once per state", {
  get_acs <- mock(data.table(), cycle = TRUE)
  get_decennial <- mock(data.table(), cycle = TRUE)
  stub(census_get_data,"get_acs",get_acs)
  stub(census_get_data,"get_decennial",get_decennial)
  stub(census_get_data,"furrr::future_map",purrr::map)
  num_fips_codes <- length(unique(tidycensus::fips_codes$state_code))

  census_get_data(2000,"sf1","first_variable")
  expect_length(mock_args(get_acs),0)
  expect_length(mock_args(get_decennial),num_fips_codes)
  census_get_data(2000,"acs5/profile","first_variable")
  expect_length(mock_args(get_acs),num_fips_codes)

})

test_that("census_get_data_all calls census_get_data once per combination of year and dataset", {
  census_get_data <- mock(data.table(), cycle = TRUE)
  stub(census_get_data_all,"census_get_data",census_get_data)

  census_get_data_all(census_variables)
  expect_length(mock_args(census_get_data), nrow(census_variables[,1,by=c("year","dataset")]))
})
