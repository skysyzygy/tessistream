withr::local_options(future.globals.maxSize = 1024^3)

# create large census test fixtures
if(!file.exists(rprojroot::find_testthat_root_file("census_data.sqlite")) ||
   !file.exists(rprojroot::find_testthat_root_file("census_features.Rds")))
  address_census_prepare_fixtures()
