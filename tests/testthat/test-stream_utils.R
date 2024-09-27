withr::local_package("mockery")
withr::local_package("checkmate")

# setnafill ---------------------------------------------------------------

x <- sample(1:26, 1e3, replace = TRUE)
x[sample(1e3, 9e2)] <- NA
x <- data.table::as.data.table(matrix(x, 1e2, 10))

test_that("setnafill errors if x is not a data.table", {
  x <- data.table::copy(x)
  expect_error(setnafill(as.data.frame(x)), "Must be a data.table")
})
test_that("setnafill errors if type is not `const`, `locf` or `nocb`", {
  x <- data.table::copy(x)
  expect_error(setnafill(x, "something"), "should be.+const.+locf.+nocb")
})
test_that("setnafill errors if cols are not in x", {
  x <- data.table::copy(x)
  expect_error(setnafill(x, cols = c("something", "V1")), "subset of.+V1.+V10", )
  expect_error(setnafill(x, cols = 1:11), "not <= 10")
})
test_that("setnafill errors if by is not in x", {
  x <- data.table::copy(x)
  expect_error(setnafill(x, type = "locf", by = c("something", "V1")), "subset of.+V1.+V10", )
})
test_that("setnafill warns if we are trying a const/group fill", {
  x <- data.table::copy(x)
  expect_warning(setnafill(x, type = "const", by = c("V1")), "argument.+by.+ignored.+const")
})
test_that("setnafill warns if we are trying a locf/nocb and we gave fill", {
  x <- data.table::copy(x)
  expect_warning(setnafill(x, type = "locf", fill = 1), "argument.+fill.+ignored.+const")
  expect_warning(setnafill(x, type = "nocb", fill = 1), "argument.+fill.+ignored.+const")
})

test_that("setnafill const matches data.table::setnafill and doesn't copy", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  tracemem(x)
  tracemem(y)
  setnafill(x, type = "const", fill = 1)
  data.table::setnafill(y, type = "const", fill = 1)
  untracemem(x)
  untracemem(y)
  expect_equal(x, y)
})
test_that("setnafill locf matches data.table::setnafill and doesn't copy", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  tracemem(x)
  tracemem(y)
  setnafill(x, type = "locf")
  data.table::setnafill(y, type = "locf")
  untracemem(x)
  untracemem(y)
  expect_equal(x, y)
})
test_that("setnafill nocb matches data.table::setnafill and doesn't copy", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  tracemem(x)
  tracemem(y)
  setnafill(x, type = "nocb")
  data.table::setnafill(y, type = "nocb")
  untracemem(x)
  untracemem(y)
  expect_equal(x, y)
})

char_cols <- 1:3
cols <- 1:6
test_that("setnafill const works with characters and factors", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  x <- x[, (char_cols) := lapply(.SD, function(.) {
    letters[.]
  }), .SDcols = char_cols]
  xf <- data.table::copy(x)[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
  setnafill(x, cols = char_cols, type = "const", fill = "z")
  setnafill(xf, cols = char_cols, type = "const", fill = "z")
  x <- x[, (char_cols) := lapply(.SD, function(.) {
    match(., letters)
  }), .SDcols = char_cols]
  xf <- data.table::copy(x)[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
  expect_equal(x, data.table::setnafill(y, cols = char_cols, type = "const", fill = 26))
  expect_equal(xf, data.table::setnafill(y, cols = char_cols, type = "const", fill = 26))
})
test_that("setnafill locf works with characters and factors", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  x <- x[, (char_cols) := lapply(.SD, function(.) {
    letters[.]
  }), .SDcols = char_cols]
  xf <- data.table::copy(x)[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
  setnafill(x, cols = cols, type = "locf")
  setnafill(xf, cols = cols, type = "locf")
  x <- x[, (char_cols) := lapply(.SD, function(.) {
    match(., letters)
  }), .SDcols = char_cols]
  xf <- data.table::copy(x)[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
  expect_equal(x, data.table::setnafill(y, cols = cols, type = "locf"))
  expect_equal(xf, data.table::setnafill(y, cols = cols, type = "locf"))
})
test_that("setnafill nocb works with characters and factors", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  x <- x[, (char_cols) := lapply(.SD, function(.) {
    letters[.]
  }), .SDcols = char_cols]
  xf <- data.table::copy(x)[, (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
  setnafill(x, cols = cols, type = "nocb")
  setnafill(xf, cols = cols, type = "nocb")
  x <- x[, (char_cols) := lapply(.SD, function(.) {
    match(., letters)
  }), .SDcols = char_cols]
  xf <- data.table::copy(x)[, (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
  expect_equal(x, data.table::setnafill(y, cols = cols, type = "nocb"))
  expect_equal(xf, data.table::setnafill(y, cols = cols, type = "nocb"))
})

x$group <- rep(seq(nrow(x) / 10), each = 10)

test_that("setnafill locf works with groups", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  expect_mapequal(
    setnafill(x, cols = cols, type = "locf", by = "group"),
    y[, (cols) := data.table::nafill(.SD, type = "locf"), by = "group", .SDcols = cols]
  )
})
test_that("setnafill nocb works with groups", {
  x <- data.table::copy(x)
  y <- data.table::copy(x)
  expect_mapequal(
    setnafill(x, cols = cols, type = "nocb", by = "group"),
    y[, (cols) := data.table::nafill(.SD, type = "nocb"), by = "group", .SDcols = cols]
  )
})


# stream_from_audit -------------------------------------------------------

# ids overlap but don't match
# columns overlap but don't match
# items missing at random

base_table <- data.table(group_customer_no = 0,
                         customer_no = 0,
                         id = seq(100),
                         create_dt = lubridate::as_datetime(seq(100)),
                         created_by = "username_base_table",
                         last_update_dt = lubridate::as_datetime(seq(100) + 1000),
                         last_updated_by = "username_base_table",
                         a = sample(letters,100,replace=T),
                         b = sample(letters,100,replace=T),
                         c = sample(letters,100,replace=T),
                         x = sample(letters,100,replace=T))


# $action <string>
# $alternate_key <string>
# $cg_key <int32>
# $column_updated <string>
# $date <timestamp[us, tz=UTC]>
# $location <string>
# $new_value <string>
# $old_value <string>
# $table_name <string>
# $userid <string>
# $customer_no <int32>
# $group_customer_no <int32>

audit_table <- data.table(group_customer_no = 0,
                    customer_no = 0,
                    date = lubridate::as_datetime(rep(seq(51,150),3) + runif(300,1,86400)),
                    new_value = sample(letters,300,replace=T),
                    old_value = sample(letters,300,replace=T),
                    alternate_key = rep(seq(51,150),3),
                    userid = "username_audit",
                    action = "Updated",
                    table_name = "T_DUMMY",
                    column_updated = sample(c("a","b","c","d"),300,replace=T))

# expand.grid(seq(nrow(base_table)),c("a","b","c","x")) %>% dplyr::sample_frac(.25) %>%
#   split(.$Var2) %>% purrr::imap(~base_table[.x$Var1,(.y):=NA])
#
# audit_table <- dplyr::sample_frac(audit_table,.75)
# base_table <- dplyr::sample_frac(base_table,.9)
#
# expand.grid(seq(nrow(base_table)),c("old_value","new_value")) %>% dplyr::sample_frac(.25) %>%
#   split(.$Var2) %>% purrr::imap(~audit_table[.x$Var1,(.y):=NA])

dummy_list_tables <- data.table(short_name = "dummy",
                                base_table = "T_DUMMY",
                                primary_keys = "id")

stub(stream_from_audit, "tessi_list_tables", dummy_list_tables)

test_that("stream_from_audit matches up table names and primary key for audit table and read_tessi", {
  read_tessi <- mock(audit_table, base_table)
  stub(stream_from_audit, "read_tessi", read_tessi)
  stub(stream_from_audit, "dcast", function(...) {rlang::abort("dcast",class="dcast")})
  expect_error(stream_from_audit("blah"), "Can't parse table_name.+blah")
  expect_error(stream_from_audit("dummy"),class="dcast")
  expect_length(mock_args(read_tessi),2)
  expect_equal(mock_args(read_tessi)[[2]][[1]],"dummy")
})


test_that("stream_from_audit builds a stream using all rows from audit and base table and all columns in audit table", {
  read_tessi <- mock(audit_table, base_table)
  stub(stream_from_audit, "read_tessi", read_tessi)
  stream <- stream_from_audit("dummy")
  expect_equal(nrow(stream), 2*nrow(base_table)+nrow(audit_table))
  checkmate::expect_names(names(stream),must.include=c("a","b","c","d"),disjunct.from = c("x"))
})

test_that("stream_from_audit has creation data from base_table overwritten by audit old_value", {
  read_tessi <- mock(audit_table, base_table)
  stub(stream_from_audit, "read_tessi", read_tessi)

  stream <- stream_from_audit("dummy")
  creation <- stream[event_subtype=="Creation"]

  creation_data <- creation[,.(id,a,b,c,d)] %>% data.table::melt(id.vars="id")
  base_data <- base_table[,.(id,a,b,c)] %>% data.table::melt(id.vars="id")

  setkey(audit_table,alternate_key,date)
  audit_data <- audit_table[,.SD[1,.(old_value = coalesce(old_value,""))],
                            by=c("alternate_key","column_updated")]

  comparison <- merge(merge(creation_data,base_data,by=c("id","variable"),all=T,suffix=c("",".base")),
                      audit_data,by.x=c("id","variable"),by.y=c("alternate_key","column_updated"),all.x=T)

  # if there's audit data, it goes to creation row
  expect_equal(comparison[!is.na(old_value) & (old_value==value)],
               comparison[!is.na(old_value)])

  # otherwise, latest data goes it to creation row
  expect_equal(comparison[is.na(old_value) & (value.base==value)],
               comparison[is.na(old_value) & !is.na(value.base)])
})

test_that("stream_from_audit has current data from base_table overwritten by audit new_value", {
  read_tessi <- mock(audit_table, base_table)
  stub(stream_from_audit, "read_tessi", read_tessi)

  stream <- stream_from_audit("dummy")
  current <- stream[event_subtype=="Current"]

  current_data <- current[,.(id,a,b,c,d)] %>% data.table::melt(id.vars="id")
  base_data <- base_table[,.(id,a,b,c)] %>% data.table::melt(id.vars="id")

  setkey(audit_table,alternate_key,date)
  audit_data <- audit_table[,.SD[.N,.(new_value = coalesce(new_value,""))],
                            by=c("alternate_key","column_updated")]

  comparison <- merge(merge(current_data,base_data,by=c("id","variable"),all=T,suffix=c("",".base")),
                      audit_data,by.x=c("id","variable"),by.y=c("alternate_key","column_updated"),all.x=T)

  # if there's base_table data, it goes to current row
  expect_equal(comparison[!is.na(value.base) & (value.base==value)],
               comparison[!is.na(value.base)])

  # otherwise, latest audit data goes to current row
  expect_equal(comparison[is.na(value.base) & (new_value==value)],
               comparison[is.na(value.base) & !is.na(new_value)])
})

test_that("stream_from_audit has all audit changes and carries them forward", {
  read_tessi <- mock(audit_table, base_table)
  stub(stream_from_audit, "read_tessi", read_tessi)

  stream <- stream_from_audit("dummy")
  change <- stream[event_subtype=="Change"]

  change_data <- change[,.(id,a,b,c,d,timestamp)] %>% data.table::melt(id.vars=c("id","timestamp"))
  base_data <- base_table[,.(id,a,b,c)] %>% data.table::melt(id.vars="id")

  comparison <- audit_table[change_data,on=c("alternate_key"="id","column_updated"="variable","date"="timestamp"),roll=Inf]
  expect_equal(comparison[new_value!=value,.N],0)
})

test_that("stream_from_audit handles extra cols and renaming of cols using the `cols` argument", {
  read_tessi <- mock(audit_table, base_table[,.(id,apple = a,bacon = b,xylophone = x)])
  stub(stream_from_audit, "read_tessi", read_tessi)

  stream <- stream_from_audit("dummy", cols = c("a"="apple","b"="bacon","x"="xylophone"))

  expect_named(stream,c("id","timestamp","event_subtype","group_customer_no","customer_no","action","last_updated_by","a","b","c","d","x"),
               ignore.order = TRUE)
  expect_lte(stream[!is.na(c) & !is.na(d),.N], stream[!is.na(a) & !is.na(b) & !is.na(x),.N])
})


# setunite -------------------------------------------------------------------

test_that("setunite combines named columns", {
  data <- data.table(a = letters, b = LETTERS)
  setunite(data, "c", a, b, sep = "!")
  expect_equal(data, data.table(c=paste(letters, LETTERS, sep = "!")))
})

test_that("setunite combines named columns tidily", {
  data <- data.table(a = letters, b = LETTERS)
  col <- "b"
  setunite(data, "c", dplyr::all_of(c("a", col)), sep = "!")
  expect_equal(data, data.table(c=paste(letters, LETTERS, sep = "!")))
})

test_that("setunite combines named columns with NAs", {
  data <- data.table(a = letters, b = NA_character_)
  setunite(data, "c", a, b, sep = "!")
  expect_equal(data, data.table(c=paste(letters, NA, sep = "!")))

  data <- data.table(a = letters, b = NA_character_)
  setunite(data, "c", a, b, sep = "!", na.rm = TRUE)
  expect_equal(data, data.table(c=letters))
})

test_that("setunite keeps old columns around if remove = FALSE", {
  data <- data.table(a = letters, b = LETTERS)
  setunite(data, "c", a, b, sep = "!", remove = FALSE)
  expect_equal(data, data.table(a = letters, b = LETTERS, c=paste(letters, LETTERS, sep = "!")))
})


# stream_customer_history -------------------------------------------------

# Loads the last row from `stream` after sorting by `timestamp` per group defined by the entries in columns `cols`,
# but only looking at timestamps before `before_date` and returns only columns matching `pattern`
#
test_that("stream_customer_history loads the last row from stream per column",{
  num_rows <- 10000
  data <- data.table(timestamp = as.POSIXct(runif(num_rows, as.POSIXct("2000-01-01"), Sys.time())),
                     group_1 = sample(letters, num_rows, replace = TRUE),
                     group_2 = sample(letters, num_rows, replace = TRUE),
                     feature_1 = seq(num_rows),
                     feature_2 = rep(LETTERS, length.out = num_rows))

  history <- stream_customer_history(data, by = c("group_1", "group_2"))

  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_setequal(history$timestamp, data[,max(timestamp),by = c("group_1", "group_2")]$V1)

  history <- stream_customer_history(arrow::as_arrow_table(data), by = c("group_1", "group_2"))

  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_equal(sort(history$timestamp), sort(data[,max(timestamp),by = c("group_1", "group_2")]$V1),
               tolerance = 1e-6, ignore_attr = "tzone")
})


test_that("stream_customer_history only returns rows before before_date",{
  num_rows <- 10000
  data <- data.table(timestamp = as.POSIXct(runif(num_rows, as.POSIXct("2000-01-01"), Sys.time())),
                     group_1 = sample(letters, num_rows, replace = TRUE),
                     group_2 = sample(letters, num_rows, replace = TRUE),
                     feature_1 = seq(num_rows),
                     feature_2 = rep(LETTERS, length.out = num_rows))

  history <- stream_customer_history(data, by = c("group_1", "group_2"), before = as.POSIXct("2010-01-01"))
  data <- data[timestamp < as.POSIXct("2010-01-01")]

  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_setequal(history$timestamp, data[,max(timestamp),by = c("group_1", "group_2")]$V1)

  history <- stream_customer_history(arrow::as_arrow_table(data), by = c("group_1", "group_2"), before = as.POSIXct("2010-01-01"))

  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_equal(sort(history$timestamp), sort(data[,max(timestamp),by = c("group_1", "group_2")]$V1),
               tolerance = 1e-6, ignore_attr = "tzone")
})

test_that("stream_customer_history only returns columns matching pattern",{
  num_rows <- 10000
  data <- data.table(timestamp = as.POSIXct(runif(num_rows, as.POSIXct("2000-01-01"), Sys.time())),
                     group_1 = sample(letters, num_rows, replace = TRUE),
                     group_2 = sample(letters, num_rows, replace = TRUE),
                     feature_1 = seq(num_rows),
                     feature_2 = rep(LETTERS, length.out = num_rows))

  history <- stream_customer_history(data, by = c("group_1", "group_2"), pattern = "2$")

  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_setequal(history$timestamp, data[,max(timestamp),by = c("group_1", "group_2")]$V1)
  expect_names(colnames(history), permutation.of = colnames(data)[-4])

  history <- stream_customer_history(arrow::as_arrow_table(data), by = c("group_1", "group_2"), pattern = "2$")

  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_equal(sort(history$timestamp), sort(data[,max(timestamp),by = c("group_1", "group_2")]$V1),
               tolerance = 1e-6, ignore_attr = "tzone")
  expect_names(colnames(history), permutation.of = colnames(data)[-4])

})

test_that("stream_customer_history loads the last row from stream per column from an arrow table",{
  num_rows <- 10000
  data <- data.table(timestamp = as.POSIXct(runif(num_rows, as.POSIXct("2000-01-01"), Sys.time())),
                     group_1 = sample(letters, num_rows, replace = TRUE),
                     group_2 = sample(letters, num_rows, replace = TRUE),
                     feature_1 = seq(num_rows),
                     feature_2 = rep(LETTERS, length.out = num_rows)) %>% as_arrow_table()
  
  history <- stream_customer_history(data, by = c("group_1", "group_2")) %>% collect %>% setDT
  data <- data %>% collect %>% setDT
  
  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_setequal(history$timestamp, data[,max(timestamp),by = c("group_1", "group_2")]$V1)
  
  history <- stream_customer_history(arrow::as_arrow_table(data), by = c("group_1", "group_2"))
  
  expect_equal(nrow(history), uniqueN(data, by = c("group_1", "group_2")))
  expect_equal(sort(history$timestamp), sort(data[,max(timestamp),by = c("group_1", "group_2")]$V1),
               tolerance = 1e-6, ignore_attr = "tzone")
})
