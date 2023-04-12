
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
