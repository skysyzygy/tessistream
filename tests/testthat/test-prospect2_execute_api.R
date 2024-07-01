withr::local_package("mockery")

expect_message_n <- function(n,...) {
  if(n == 1) { expect_message(...) }
  else { expect_message(expect_message_n(n = n-1, ...)) }
}

# p2_execute_api ----------------------------------------------------------

test_that("p2_execute_api sends object to url using method and logs info", {
  POST <- mock(list(status_code = 200),cycle=T)
  PUT <- mock(list(status_code = 200),cycle=T)
  stub(p2_execute_api, "httr::POST", POST)
  stub(p2_execute_api, "httr::PUT", PUT)

  expect_message(p2_execute_api("http://api.url", list("json" = "object")), "v Executing POST : http://api.url")
  expect_message(p2_execute_api("http://api.url", list("json" = "object"), method = "PUT"), "v Executing PUT : http://api.url")
  expect_message(p2_execute_api("http://api.url", list("json" = "object")), '* \\{"json":"object"\\}')

  expect_equal(mock_args(POST)[[1]][["url"]],"http://api.url")
  expect_equal(mock_args(POST)[[1]][["body"]],list("json" = "object"))
  expect_length(mock_args(POST),2)
  expect_length(mock_args(PUT),1)
})


test_that("p2_execute_api executes the command only if dry_run is FALSE", {
  PUT <- mock(list(status_code = 200),cycle=T)
  stub(p2_execute_api, "httr::POST", PUT)

  expect_message_n(2,p2_execute_api("http://api.url", list("json" = "object"), dry_run = T),"* \\(dry run\\)")
  expect_length(mock_args(PUT),0)

  expect_message(p2_execute_api("http://api.url", list("json" = "object"), dry_run = F))
  expect_length(mock_args(PUT),1)
})

test_that("p2_execute_api returns T/F and warns based on return code", {
  POST <- mock(list(status_code = 200),cycle=T)
  stub(p2_execute_api, "httr::POST", POST)
  DELETE <- mock(list(status_code = 400),cycle=T)
  stub(p2_execute_api, "httr::DELETE", DELETE)

  expect_message_n(2,expect_true(p2_execute_api("http://api.url", list("json" = "object"), dry_run = TRUE)))
  expect_message(expect_true(p2_execute_api("http://api.url", list("json" = "object"), method = "POST")))
  expect_warning(expect_message(expect_false(p2_execute_api("http://api.url", list("json" = "object"), method = "DELETE"))))
  expect_message(expect_true(p2_execute_api("http://api.url", list("json" = "object"), method = "DELETE", success_codes = 400)))
})

# p2_verb_thing --------------------------------------------------------------

tags <- readRDS(rprojroot::find_testthat_root_file("tags.Rds"))

test_that("p2_verb_thing gets the thing id and warns if it can't find it", {
  p2_query_api <- mock(tags, cycle = TRUE)
  stub(p2_verb_thing, "p2_query_api", p2_query_api)

  p2_execute_api <- mock(TRUE, cycle = TRUE)
  stub(p2_verb_thing, "p2_execute_api", p2_execute_api)

  p2_verb_thing(c("one","two"),
                  verb = "PUT", thing = "tag")

  expect_length(mock_args(p2_query_api),1)
  expect_match(mock_args(p2_query_api)[[1]][[1]],"api/3/tags")
  expect_warning(p2_verb_thing(c("four","five"),
                verb = "PUT", thing = "tag"),"Cannot find.+five")
})

test_that("p2_verb_thing calls p2_execute_api", {
  p2_query_api <- mock(tags, cycle = TRUE)
  stub(p2_verb_thing, "p2_query_api", p2_query_api)

  p2_execute_api <- mock(TRUE, cycle = TRUE)
  stub(p2_verb_thing, "p2_execute_api", p2_execute_api)

  p2_verb_thing(c("one","two"),map(c("three","four"),
                                   ~list(tag=list(tag=.))),
                  verb = "PUT", thing = "tag",
                  dry_run = TRUE)

  p2_object = jsonlite::fromJSON('{"tag":{"tag": "three"}}')

  expect_length(mock_args(p2_execute_api),2)
  expect_match(mock_args(p2_execute_api)[[1]][["url"]],paste0(api_url,"/api/3/tags/1$"))
  expect_equal(mock_args(p2_execute_api)[[1]][["object"]],p2_object)
  expect_equal(mock_args(p2_execute_api)[[1]][["dry_run"]], TRUE)
})
