withr::local_package("progressr")
withr::local_package("mockery")

future::plan("multisession")
handlers("cli")
tessilake:::local_cache_dirs()
stub(p2_update,"p2_load",function(...) {
  args <- modifyList(list2(...), list(max_len = 100))
  do.call(tessistream:::p2_load, args)
  })

test_that("p2_update is able to query all tables", {
  with_progress(
    expect_silent(p2_update())
  )
})
