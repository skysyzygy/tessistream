withr::local_package("mockery")
withr::local_package("lubridate")
future::plan("multisession")
tessilake:::local_cache_dirs()
dir.create(file.path(tempdir(),"deep","stream"))

max_len <- 2000

stub(p2_update,"p2_load",function(table, query = NULL, ...) {
  if(table == "contacts")
    query = list("filters[updated_after]" = as.character(today() - ddays(1)))
  args <- modifyList(list2(...), list(table = table,
                                      max_len = max_len,
                                      query = query))
  do.call(tessistream:::p2_load, args)
  })

test_that("p2_update is able to query all tables", {
  progressr::with_progress(p2_update(),progressr::handler_pbcol())
  p2_db_open()
  withr::defer(p2_db_close())

  tables <- c("bounceLogs",
              "campaigns",
              "contactLists",
              "contacts",
              "fieldValues",
              "linkData",
              "links",
              "lists",
              "logs",
              "messages",
              "mppLinkData")

  table_lens <- map(tables, ~ p2_query_table_length(file.path(api_url,"api/3/",.)))

  expect_contains(DBI::dbListTables(tessistream$p2_db), tables)
  walk2(tables,table_lens,
       ~ expect_gte(tbl(tessistream$p2_db, !!.x) %>% dplyr::tally() %>% collect %>% .[[1]],
                      pmin(.y, max_len)))

})
