dataset <- rep(list(sample(letters, 1e6, replace = TRUE)),26) %>% setDT %>% setnames(letters)

test_that("setunite is faster than unite for large datasets when not dropping NA", {

  time_unite <- system.time(
    result_unite <- tidyr::unite(dataset, "c", a, b) %>% setDT
  )
  time_setunite <- system.time(
    result_setunite <- setunite(dataset, "c", a, b)
  )

  print(time_unite)
  print(time_setunite)
  expect_equal(result_setunite, result_unite)
  expect_lt(time_setunite[1], time_unite[1])
  expect_lt(time_setunite[3], time_unite[3])
})

dataset <- rep(list(sample(letters, 1e6, replace = TRUE)),26) %>% setDT %>% setnames(letters)

test_that("setunite is faster than unite for large datasets when dropping NA", {

  time_unite <- system.time(
    result_unite <- tidyr::unite(dataset, "c", a, b, na.rm = TRUE) %>% setDT
  )

  time_setunite <- system.time(
    result_setunite <- setunite(dataset, "c", a, b, na.rm = TRUE)
  )

  print(time_unite)
  print(time_setunite)
  expect_equal(result_setunite, result_unite)
  expect_lt(time_setunite[1], time_unite[1])
  expect_lt(time_setunite[3], time_unite[3])
})
