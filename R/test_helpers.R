address_geocode_prepare_fixtures <- function() {
  tessilake:::local_cache_dirs()

  address_stream <- data.table(
    street1 = c("30 Lafayette Ave", "321 Ashland Pl", "30 Churchill Pl"),
    street2 = NA_character_,
    city = c("Brooklyn", "Brooklyn", "London"),
    state = c("NY", "NY", NA_character_),
    country = c("USA", "USA", "UK"),
    postal_code = c("11217", "11217", "E14 5EU")
  )

  address_geocode(address_stream) %>%
    saveRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))
}

address_census_prepare_fixtures <- function() {
  stub <- NULL
  withr::local_package("mockery")

  .census_data <- function(...) {
    census_data(db_name = rprojroot::find_testthat_root_file("census_data.sqlite"), ...)
  }

  # only load data from NY
  stub(census_get_data, "unique", "NY")
  stub(census_get_data_all, "census_get_data", census_get_data)
  stub(census_data, "census_get_data_all", census_get_data_all)

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

  saveRDS(census_features(), rprojroot::find_testthat_root_file("census_features.Rds"))
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

  addresses <- read_tessi("addresses") %>%
    dplyr::semi_join(audit, by = c("address_no" = "alternate_key")) %>%
    collect() %>%
    setDT()

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

  mockery::stub(stream_from_audit, "read_tessi", mockery::mock(audit, addresses))
  mockery::stub(address_create_stream, "stream_from_audit", stream_from_audit)

  stream <- address_create_stream()

  future::plan("multisession")
  address_parse <- address_parse(stream)

  file.copy(tessilake::cache_primary_path("address_stream.sqlite","stream"), testthat::test_path(), overwrite = T)
  saveRDS(stream, testthat::test_path("address_stream.Rds"))
  saveRDS(audit, testthat::test_path("address_audit.Rds"))
  saveRDS(addresses, testthat::test_path("addresses.Rds"))
}

#' @importFrom stats runif
contributions_stream_prepare_fixtures <- function() {
  . <- memb_amt <- start_amt <- recog_amt <- AVC_amt <- end_amt <- cont_amt <-
    group_customer_no <- cust_memb_no <- create_dt <- init_dt <- cont_dt <-
    campaign_category_desc <- NULL

  # simulate some memberships
  n_memberships <- 1000
  memberships <- data.table(
    group_customer_no = sample(seq(100), n_memberships, replace = TRUE),
    start_amt = sample(seq(0, 10000, 1000), n_memberships, replace = TRUE),
    campaign_category_desc = sample(paste("Campaign", LETTERS), n_memberships, replace = TRUE),
    cust_memb_no = seq(n_memberships),
    AVC_amt = sample(seq(0, 500, 100), n_memberships, replace = TRUE)
  ) %>%
    .[, memb_amt := start_amt * sample(c(1 / 2, 1, 2), .N, replace = TRUE)] %>%
    .[, recog_amt := memb_amt + AVC_amt] %>%
    .[, end_amt := start_amt + 999.99] %>%
    .[, `:=`(
      create_dt = today() + lubridate::dyears(seq(.N)) + lubridate::ddays(runif(.N, -365, 30)),
      init_dt = today() + lubridate::dyears(seq(.N))
    ), by = "group_customer_no"]

  contributions <- memberships[, .(group_customer_no,
    campaign_no = sample(10, .N, replace = TRUE),
    cust_memb_no = NA_integer_,
    cust_memb_no_real = cust_memb_no,
    ref_no = seq(.N),
    type = sample(c(1, 2, 3), .N, replace = TRUE),
    create_dt = create_dt + lubridate::ddays(runif(.N, -365, 30)),
    cont_dt = init_dt + lubridate::ddays(runif(.N, -90, 180)),
    campaign_category_desc
  )] %>%
    .[, cont_amt := case_when(
      type == 1 ~ memberships[, memb_amt + AVC_amt],
      type == 2 ~ memberships[, recog_amt],
      type == 3 ~ memberships[, start_amt + AVC_amt]
    )]

  # split up some contributions
  contributions[1:50, cont_amt := cont_amt / 2]
  contributions <- rbind(contributions, contributions[1:50] %>%
    .[, `:=`(
      create_dt = create_dt + 90,
      cont_dt = cont_dt + 90,
      ref_no = nrow(contributions) + 1:50,
      type = 5
    )])

  # add extra dummy contributions
  contributions <- rbind(contributions, data.table(
    group_customer_no = sample(seq(100), n_memberships, replace = TRUE),
    campaign_no = sample(10, n_memberships, replace = TRUE),
    cust_memb_no = NA_integer_,
    cust_memb_no_real = NA_integer_,
    ref_no = seq(n_memberships) + nrow(contributions),
    type = 4,
    create_dt = today() + lubridate::ddays(runif(n_memberships, -365, 30)),
    cont_dt = today() + lubridate::ddays(runif(n_memberships, -90, 180)),
    campaign_category_desc = sample(paste("Campaign", LETTERS), n_memberships, replace = TRUE),
    cont_amt = sample(seq(0, 500, 100), n_memberships, replace = TRUE)
  ))

  saveRDS(contributions, "tests/testthat/contribution_stream-contributions.Rds")
  saveRDS(memberships, "tests/testthat/contribution_stream-memberships.Rds")
}

duplicates_prepare_fixtures <- function() {
  withr::local_envvar(R_CONFIG_FILE="")

  address_stream <- readRDS(rprojroot::find_testthat_root_file("address_stream.Rds"))

  customers <- read_tessi("customers", select = c("cust_type_desc", "inactive_desc", "fname", "lname", "customer_no")) %>%
    select(c("cust_type_desc", "inactive_desc", "fname", "lname", "customer_no", "group_customer_no")) %>%
    collect %>% setDT %>%
    .[runif(.N)<.01]
  # anonymize customer # and scramble names
  customers[, `:=`(group_customer_no = sample(address_stream$customer_no, .N, replace = T),
                   fname = sample(fname, .N),
                   lname = sample(lname, .N))] %>%
    .[,customer_no := group_customer_no]

  saveRDS(customers, rprojroot::find_testthat_root_file("duplicates_stream-customers.Rds"))

  # make some random phones and emails
  phones <- data.table(group_customer_no = address_stream$group_customer_no,
                       phone = as.character(floor(runif(nrow(address_stream), 1e9, 1e10-.1))))

  emails <- data.table(group_customer_no = address_stream$group_customer_no,
                       eaddress_type = 1,
                       address = paste0(
                         sample(letters, nrow(address_stream), replace = TRUE),
                         gsub("[^a-zA-Z]+", "_", sample(trimws(customers$lname), nrow(address_stream), replace = TRUE)),
                         "@",
                         c("google.com","yahoo.com","aol.com","bam.org")))

  saveRDS(phones, rprojroot::find_testthat_root_file("duplicates_stream-phones.Rds"))
  saveRDS(emails, rprojroot::find_testthat_root_file("duplicates_stream-emails.Rds"))

}

email_prepare_fixtures <- function() {
  n_rows = 100000
  withr::local_package("lubridate")

  promotions = data.frame(
    media_type = 3,
    customer_no = sample(1:1000,n_rows,replace = TRUE),
    promote_dt = as_datetime(runif(n_rows, ymd_hms("2000-01-01 00:00:00"), now())),
    appeal_no = sample(1:10, n_rows, replace = TRUE),
    campaign_no = sample(1:10, n_rows, replace = TRUE),
    source_no = sample(1:100, n_rows, replace = TRUE)) %>%
  mutate(group_customer_no = customer_no + 10000,
         eaddress = ifelse(runif(n_rows) < .1,
                           paste(customer_no,c("mac.com","me.com","hotmail.com",
                                               "mac.COM ","ME.com ","hotmail.com "),sep = "@"),
                           NA))

  ### Promotion responses

  promotion_responses =
    filter(promotions, !is.na(eaddress)) %>%
    filter(source_no > 1) %>% # one source has no response
      transmute(group_customer_no,customer_no,
              response = sample(1:5, nrow(.), replace = TRUE),
              response_dt = promote_dt + runif(nrow(.), 300, 86400),
              source_no, url_no = sample(1:1000, nrow(.), replace = TRUE))

  arrow::write_parquet(promotions, rprojroot::find_testthat_root_file("email_stream-promotions.parquet"))
  arrow::write_parquet(promotion_responses, rprojroot::find_testthat_root_file("email_stream-promotion_responses.parquet"))

  emails <- data.frame(customer_no = sample(1:1000,n_rows, replace = TRUE),
                       primary_ind = "Y",
                       timestamp = as_datetime(runif(n_rows, ymd_hms("2000-01-01 00:00:00"), now()))) %>%
    mutate(group_customer_no = customer_no + 10000,
           address = paste(customer_no,sample(c("gmail.com","yahoo.com","bam.org"),n_rows,replace = TRUE),
                           sep = "@")) %>% setDT

  saveRDS(emails, rprojroot::find_testthat_root_file("email_stream-emails.Rds"))


}


p2_prepare_fixtures <- function() {
  stub <- mutate <- across <- any_of <- NULL

  withr::local_dir(rprojroot::find_testthat_root_file())
  withr::local_package("mockery")
  withr::local_package("dplyr")

  stub(p2_query_api, "p2_query_table_length", 100)
  stub(p2_load, "p2_query_api", p2_query_api)

  file.remove("p2.sqlite")
  p2_db_open("p2.sqlite")
  withr::defer(p2_db_close())

  tables <- c("fieldValues", "campaigns", "messages", "links", "lists", "bounceLogs", "contactLists", "contacts", "logs", "linkData", "mppLinkData")
  map(tables[-1], p2_load)
  p2_load("fieldValues", path = "api/3/fieldValues", query = list("filters[fieldid]" = 1))

  # anonymize
  anonymized_emails <- paste0(seq(100), "@gmail.com")

  lapply(tables, function(table) {
    tbl(tessistream$p2_db, table) %>%
      collect() %>%
      mutate(across(any_of(c("email", "email_local")), ~ ifelse(!is.na(.) & . != "", anonymized_emails, ""))) %>%
      mutate(across(any_of("email_domain"), ~ ifelse(!is.na(.) & . != "", anonymized_emails, "gmail.com"))) %>%
      mutate(across(any_of(c("firstName", "lastName", "first_name", "last_name", "to_name")), ~"")) %>%
      copy_to(dest = tessistream$p2_db, table, overwrite = TRUE, temporary = FALSE)
  })

  invisible()
}
