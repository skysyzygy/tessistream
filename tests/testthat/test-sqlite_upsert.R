db <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
withr::defer({
  DBI::dbDisconnect(db)
})

test_that("sqlite_upsert errors if a table doesn't exist or doesn't have index columns", {
  df <- data.frame(a = seq(100))
  expect_error(sqlite_upsert(db, "test", df), "Must be element of set")
  dplyr::copy_to(db, df, "test", temporary = F)
  expect_error(sqlite_upsert(db, "test", df), "index_cols.+length >= 1")
  dplyr::copy_to(db, df, "test", unique_indexes = list("a"), overwrite = T, temporary = F)
  sqlite_upsert(db, "test", df)

  df <- data.frame(b = seq(100))
  expect_error(sqlite_upsert(db, "test", df), "names.+must include.+'a'")
})

test_that("sqlite_upsert gracefully handles data that doesn't match schema", {
  df <- data.frame(a = seq(100), b = seq(100))
  df2 <- data.frame(b = seq(100), c = seq(100))
  df3 <- data.frame(d = seq(100), e = seq(100))
  dplyr::copy_to(db, df, "test", unique_indexes = list("b"), overwrite = T, temporary = F)
  expect_warning(sqlite_upsert(db, "test", df2), "columns.+not present in table test.+c")
  expect_error(sqlite_upsert(db, "test", df3), "names.+must include.+'b'")
})

test_that("sqlite_upsert appends non-matching rows", {
  df <- data.frame(a = seq(100))
  df2 <- data.frame(a = seq(101, 200))
  dplyr::copy_to(db, df, "test", unique_indexes = list("a"), overwrite = T, temporary = F)
  sqlite_upsert(db, "test", df2)
  expect_equal(as.data.frame(dplyr::tbl(db, "test")), rbind(df, df2))
})

test_that("sqlite_upsert updates matching rows", {
  df <- data.frame(a = seq(100), b = runif(100))
  df2 <- data.frame(a = seq(100), b = runif(100))
  dplyr::copy_to(db, df, "test", unique_indexes = list("a"), overwrite = T, temporary = F)
  sqlite_upsert(db, "test", df2)
  expect_equal(as.data.frame(dplyr::tbl(db, "test")), df2)
})
