withr::local_package("mockery")

test_that("make_resilient makes expr failure resistant", {
  sleep <- mock()
  stub(make_resilient, "Sys.sleep", sleep)

  expect_message(res <- make_resilient(stop("failure!!!"), num_tries = 10, default = "we failed"),
                        "failure!!!")

  expect_length(mock_args(sleep),10)
  expect_equal(res, "we failed")

})

