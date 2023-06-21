address_geocode_prepare_fixtures <- function() {
  tessilake:::local_cache_dirs()

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave","321 Ashland Pl","30 Churchill Pl"),
    street2 = NA_character_,
    city = c("Brooklyn","Brooklyn","London"),
    state = c("NY","NY",NA_character_),
    country = c("USA","USA","UK"),
    postal_code = c("11217","11217","E14 5EU")
  )

  address_geocode(address_stream) %>%
    saveRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))
}

address_census_prepare_fixtures <- function() {
  stub <- NULL
  withr::local_package("mockery")

  .census_data <- function(...) {census_data(db_name = rprojroot::find_testthat_root_file("census_data.sqlite"), ...)}

  # only load data from NY
  stub(census_get_data,"unique","NY")
  stub(census_get_data_all,"census_get_data",census_get_data)
  stub(census_data,"census_get_data_all",census_get_data_all)

  # load data to temp database
  .census_data(census_variables())

  stub(census_race_features, "census_data", .census_data)
  stub(census_sex_features, "census_data", .census_data)
  stub(census_age_features, "census_data", .census_data)
  stub(census_income_features, "census_data", .census_data)

  stub(census_features, "census_race_features", census_race_features)
  stub(census_features, "census_sex_features", census_sex_features)
  stub(census_features, "census_age_features", census_age_features)
  stub(census_features, "census_income_features", census_income_features)

  saveRDS(census_features(),rprojroot::find_testthat_root_file("census_features.Rds"))

}

address_prepare_fixtures <- function() {
  . <- N <- table_name <- alternate_key <- address_no <- NULL
  tessilake:::local_cache_dirs()

  # only take customers with lots of changes
  audit <- read_tessi("audit") %>%
    filter(table_name == "T_ADDRESS") %>%
    dplyr::mutate(alternate_key = as.integer(alternate_key)) %>%
    collect() %>%
    setDT() %>%
    .[, N := .N, by = c("group_customer_no", "alternate_key")] %>%
    .[N > 10]

  addresses <- read_tessi("addresses") %>% collect %>% setDT %>%
    .[address_no %in% audit$alternate_key]

  # anonymize address number
  address_map <- data.table(address_no = unique(c(audit$alternate_key, addresses$address_no)))[, I := .I]
  addresses <- merge(addresses, address_map, by = "address_no") %>%
    .[, address_no := I] %>%
    .[, I := NULL]
  audit <- merge(audit, address_map, by.x = "alternate_key", by.y = "address_no") %>%
    .[, alternate_key := I] %>%
    .[, I := NULL]

  # anonymize customer number
  customer_map <- data.table(customer_no = unique(c(audit$customer_no, addresses$customer_no)))[, I := .I]
  addresses <- merge(addresses, customer_map, by = "customer_no") %>%
    .[, `:=`(customer_no = I, group_customer_no = I)] %>%
    .[, I := NULL]
  audit <- merge(audit, customer_map, by = "customer_no") %>%
    .[, `:=`(customer_no = I, group_customer_no = I)] %>%
    .[, I := NULL]

  mockery::stub(stream_from_audit, "read_tessi", mockery::mock(audit,addresses))
  mockery::stub(address_create_stream, "stream_from_audit", stream_from_audit)

  stream <- address_create_stream()

  saveRDS(stream, testthat::test_path("address_stream.Rds"))
  saveRDS(audit, testthat::test_path("address_audit.Rds"))
  saveRDS(addresses, testthat::test_path("addresses.Rds"))
}


p2_prepare_fixtures <- function() {
  stub <- mutate <- across <- any_of <- NULL

  withr::local_dir(rprojroot::find_testthat_root_file())
  withr::local_package("mockery")
  withr::local_package("dplyr")

  stub(p2_query_api,"as.integer",100)
  stub(p2_load,"p2_query_api",p2_query_api)
  p2_db_open("p2.sqlite")

  tables <- c("fieldValues","campaigns","messages","links","lists","bounceLogs","contactLists","contacts","logs","linkData")
  map(tables[-1],p2_load)
  p2_load("fieldValues", path = "api/3/fieldValues", query = list("filters[fieldid]" = 1))

  # anonymize
  anonymized_emails <- paste0(seq(100),"@gmail.com")

  lapply(tables,function(table) {
    tbl(tessistream$p2_db,table) %>% collect %>%
        mutate(across(any_of(c("email","email_local")),~ifelse(!is.na(.) & .!="",anonymized_emails,""))) %>%
        mutate(across(any_of("email_domain"),~ifelse(!is.na(.) & .!="",anonymized_emails,"gmail.com"))) %>%
        mutate(across(any_of(c("firstName","lastName","first_name","last_name","to_name")),~"")) %>%
        copy_to(dest = tessistream$p2_db,table,overwrite=T,temporary = FALSE)
  })

  p2_db_close()

}
