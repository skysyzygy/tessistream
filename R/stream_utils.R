#' setnafill
#'
#' Wrapper for data.table's setnafill for fast filling by group and non-numeric columns.
#' Displatches to setnafill_ functions based on column class and type of fill
#'
#' @param x data.table
#' @param type character, one of "`const`", "`locf`" or "`nocb`". Defaults to "`const`".
#' @param fill vector, value to be used to fill when `type=="const"`.
#' @param cols numeric or character vector specifying columns to be updated.
#' @param by character string of columns to group by
#'
#' @return data.table, filled in-place
#' @export
#'
#' @examples
#' x <- data.table::data.table(a = NA_character_, b = rep(seq(1, 2), each = 10))
#' x[1, a := "a"]
#' x[10, a := "x"]
#'
#' y <- data.table::copy(x)
#' setnafill(y, type = "const", fill = "test", cols = "a")[]
#'
#' y <- data.table::copy(x)
#' setnafill(y, type = "locf", by = "b")[]
#'
#' y <- data.table::copy(x)
#' setnafill(y, type = "nocb", by = "b")[]
#'
#' @importFrom checkmate assert_data_table assert_names assert check_names check_integer
setnafill <- function(x, type = c("const", "locf", "nocb"), fill = NA, cols = seq_along(x), by = NA) {
  assert_data_table(x)

  type <- match.arg(type)
  args <- rlang::call_args(match.call())

  if (!any(is.na(by))) {
    assert_names(by, subset.of = colnames(x))
  }

  assert(
    check_names(cols, subset.of = colnames(x)),
    check_integer(cols, lower = 1, upper = ncol(x))
  )

  if (type != "const" && !missing(fill)) {
    warning("argument 'fill' ignored, only make sense for type='const'")
    args$fill <- NULL
  }

  if (type == "const" && !missing(by)) {
    warning("argument 'by' ignored, doesn't make sense for type='const'")
    args$by <- NULL
  }

  if (is.integer(cols)) {
    cols <- colnames(x)[cols]
  }

  numeric_cols <- intersect(
    colnames(x)[sapply(x, is.numeric)],
    cols
  )

  numeric_method <- if (missing(by)) {
    data.table::setnafill
  } else {
    setnafill_group
  }

  other_cols <- setdiff(cols, numeric_cols)

  other_method <- if (type == "const") {
    setnafill_const_simple
  } else {
    setnafill_group
  }

  if (length(numeric_cols) > 0) {
    args$cols <- numeric_cols
    do.call(numeric_method, args, envir = parent.frame())
  }
  if (length(other_cols) > 0) {
    args$cols <- other_cols
    do.call(other_method, args, envir = parent.frame())
  }

  x
}

#' @describeIn setnafill Wrap data.table::setnafill with some logic to convert
#' character and factor columns to integers and back again
setnafill_factor_character <- function(x, type = "const", fill = NA, cols = seq_along(x)) {
  args <- as.list(match.call())[-1]

  classes <- lapply(x[, cols, with = F], class)
  char_fact_cols <- cols[classes %in% c("character", "factor")]
  char_cols <- cols[classes %in% "character"]

  if (length(char_fact_cols) == 0) {
    return(suppressWarnings(do.call(data.table::setnafill, args)))
  }

  x[, (char_fact_cols) := lapply(.SD, factor), .SDcols = char_fact_cols]
  levels <- lapply(x[, char_fact_cols, with = F], levels)
  x[, (char_fact_cols) := lapply(.SD, as.integer), .SDcols = char_fact_cols]

  if (!missing(fill)) {
    max_length <- max(sapply(levels, length))
    levels <- lapply(levels, function(l) {
      l[max_length + 1] <- fill
      l
    })
    fill <- max_length + 1
    data.table::setnafill(x, type = type, fill = fill, cols = cols)
  } else {
    data.table::setnafill(x, type = type, cols = cols)
  }

  x[, (char_fact_cols) := lapply(seq_along(.SD), function(i) {
    factor(.SD[[i]],
      levels = seq_along(levels[[i]]),
      labels = levels[[i]]
    )
  }), .SDcols = char_fact_cols]

  if (length(char_cols) > 0) {
    x[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
  }

  x
}

#' @describeIn setnafill Simple constant fill for factors and character
setnafill_const_simple <- function(x, type = "const", fill = NA, cols = seq_along(x)) {
  if (is.integer(cols)) {
    cols <- colnames(x)[cols]
  }
  lapply(cols, function(c) {
    x[is.na(get(c)), (c) := fill]
  })
  x
}

#' @describeIn setnafill rolling join to quickly do setnafill by groups
#' @importFrom stats na.omit
setnafill_group <- function(x, type = "locf", cols = seq_along(x), by = NA) {
  x[, I := .I]
  setkey(x, I)
  roll <- ifelse(type == "locf", Inf, -Inf)

  lapply(cols, function(col) {
    i <- is.na(x[, col, with = F][[1]])
    x[i, (col) := x[!i][x[i], col, on = na.omit(c(by, "I")), roll = roll, with = F]]
  })

  x[, I := NULL]
}

#' stream_debounce
#'
#' Takes the last row per day for each group identified by columns identified in `...`
#'
#' @param stream data.table in a stream format (needs at least a `timestamp` column)
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> columns to group by for debouncing
#'
#' @return de-bounced stream
#' @export
#'
#' @importFrom rlang list2
#' @importFrom data.table setorderv
#'
#' @examples
#' stream <- data.table::data.table(
#'   timestamp = Sys.time() + as.difftime(0:48, units = "hours"),
#'   x = 0:48, y = rep(0:4, 12)
#' )
#' stream_debounce(stream)
stream_debounce <- function(stream, ...) {
  timestamp <- NULL

  cols <- sapply(rlang::enquos(...), rlang::quo_name)
  temp_col <- digest::sha1(Sys.time())
  group <- temp_col
  if (length(cols) > 0) {
    group <- c(group, cols)
  }

  stream[, (temp_col) := lubridate::floor_date(timestamp, "day")]

  setorderv(stream, "timestamp")

  # Debounce, take the last change per group per day
  stream <- stream[, .SD[.N], by = group]

  stream[, (temp_col) := NULL]

  stream
}
