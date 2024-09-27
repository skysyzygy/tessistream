#' setnafill
#'
#' Wrapper for [data.table::setnafill] for fast filling by group and non-numeric columns.
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
#' @importFrom data.table setkey
setnafill_group <- function(x, type = "locf", cols = seq_along(x), by = NA) {
  x[, I := .I]
  setkey(x, I)
  roll <- ifelse(type == "locf", Inf, -Inf)

  .x <- x
  join_cols <- na.omit(c(by, "I"))
  lapply(cols, function(col) {
    idx <- is.na(.x[, col, with = FALSE][[1]])
    filled <- .x[!idx,c(join_cols,col),with=F] %>% 
      .[.x[idx,join_cols, with = F], col, on = join_cols, roll = roll, with = FALSE]
    .x[idx, (col) := filled]
  })

  x[, I := NULL]
}

#' stream_debounce
#'
#' Takes the last row for each group identified by columns identified in `...`
#'
#' @param stream data.table in a stream format
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> columns to group by for debouncing
#'
#' @return de-bounced stream
#' @export
#'
#' @importFrom rlang list2
#' @importFrom data.table haskey last
#' @importFrom checkmate assert_data_table
#'
#' @examples
#' stream <- data.table::data.table(
#'   x = 0:48, y = rep(0:4, 12)
#' )
#' data.table::setkey(stream, x)
#' stream_debounce(stream, "y")
stream_debounce <- function(stream, ...) {

  cols <- sapply(rlang::enquos(...), rlang::eval_tidy)

  assert_data_table(stream)
  assert_true(haskey(stream))

  # Debounce, take the last change per group per day
  stream[, last(.SD), by = cols][]

}


#' stream_from_audit
#'
#' Helper function to load data from the audit table and base table identified by `table_name`.
#' Produces a stream of creation/change/current state of all fields in the audit table in order
#' to reconstruct the state of a given element at some time in the past.
#'
#' @param table_name character table name as in `tessilake::tessi_list_tables` `short_name` or `long_name`
#' @param cols character vector of columns that will be used from the audit and base table.
#' The names of the vector are the column names as identified in the audit table, the values are the column names as identified in the base table.
#' *Default: column names from the audit table.*
#' @param ... extra arguments passed on to `tessilake::read_tessi`
#'
#' @importFrom dplyr transmute filter coalesce
#' @importFrom data.table setDT dcast setkeyv
#' @importFrom tessilake read_tessi tessi_list_tables
#' @importFrom rlang sym syms
#'
stream_from_audit <- function(table_name, cols = NULL, ...) {
  . <- primary_keys <- group_customer_no <- customer_no <- action <- timestamp <-
    new_value <- old_value <- alternate_key <- userid <- column_updated <-
    create_dt <- created_by <- last_update_dt <- last_updated_by <- event_subtype <- NULL

  tessi_tables <- tessi_list_tables()

  if (table_name %in% tessi_tables$short_name) {
    short_name <- table_name
    base_table <- tessi_tables[short_name == table_name,base_table]
    pk_name <- tessi_tables[short_name == table_name,primary_keys]
  } else if (table_name %in% tessi_tables$base_table){
    base_table <- table_name
    short_name <- tessi_tables[short_name == table_name,short_name]
    pk_name <- tessi_tables[short_name == table_name,primary_keys]
  } else {
    rlang::abort(c("Can't parse table_name, must be one defined in tessilake","*"=table_name))
  }

  if(length(pk_name) > 1)
    rlang::abort(c("Don't know how to work with a table with multiple primary keys!"))

  audit <- read_tessi("audit", ...) %>%
    filter(table_name == !!base_table) %>%
    transmute(group_customer_no,
              customer_no,
              action,
              timestamp = date,
              new_value = coalesce(new_value, ""), # this works with deletes and purges as well as updates that are zeroed out.
              old_value = ifelse(tolower(action) %in% c("inserted"),new_value,coalesce(old_value,"")), # insertions are treated as no change
              !!pk_name := as.integer(as.character(alternate_key)),
              last_updated_by = userid,
              column_updated = coalesce(column_updated, "NA")
    ) %>%
    collect() %>%
    setDT()

  setkeyv(audit,c(pk_name,"timestamp"))

  base_table <- read_tessi(short_name, ...)
  cols <- cols %||% unique(audit$column_updated)
  if(is.null(names(cols)))
    names(cols) <- cols

  stream_creation <- base_table %>%
    transmute(group_customer_no,customer_no,
              event_subtype = "Creation",
              timestamp = create_dt,
              !!sym(pk_name),
              last_updated_by = created_by) %>%
    collect() %>%
    setDT()

  stream_current <- base_table %>%
    transmute(group_customer_no,customer_no,
              event_subtype = "Current",
              timestamp = last_update_dt,
              !!sym(pk_name),
              !!!syms(cols[cols %in% colnames(base_table)]),
              last_updated_by) %>%
    collect() %>%
    setDT()

  setkey(audit, timestamp)

  audit <- stream_debounce(audit,setdiff(colnames(audit),"new_value"))

  audit_changes <- audit %>%
    dcast(... ~ column_updated, value.var = "new_value") %>%
    .[, `:=`(event_subtype = "Change",
             old_value = NULL)]

  audit_creation <- audit %>%
    .[, .SD[1], by = c(pk_name, "column_updated")] %>%
    dcast(rlang::new_formula(sym(pk_name),sym("column_updated")), value.var = "old_value") %>%
    .[, event_subtype := "Creation"]

  stream <- rbind(stream_creation, stream_current, audit_changes, fill = TRUE)

  # Data fill-in based on audit old_value -- all other old_values are captured within the audit table itself
  stream <- stream[audit_creation,
                 (names(cols)) := mget(paste0("i.",names(cols)), ifnotfound = NA_character_),
                 on = c(pk_name, "event_subtype")
  ]

  # Order event_subtype
  stream[, event_subtype := factor(event_subtype, levels = c("Creation", "Change", "Current"))]

  setkeyv(stream, c(pk_name, "event_subtype", "timestamp"))
  # Fill-down changes
  setnafill(stream, "locf", cols = names(cols), by = pk_name)
  # And then fill back up for non-changes
  setnafill(stream, "nocb", cols = names(cols), by = pk_name)

  setkeyv(stream, c(pk_name, "event_subtype", "timestamp"))

  stream_debounce(stream,!!pk_name,"timestamp")

}


#' setunite
#'
#' Convenience function to paste together multiple columns into one. Thin wrapper around [tidyr::unite]
#'
#' @param data data.table to act on
#' @param col The name of the new column, as a string or symbol.
#' @param ... `<tidy-select>` Columns to unite
#' @param sep Separator to use between values.
#' @param remove If `TRUE`, the default, remove input columns from output data frame.
#' @param na.rm If `TRUE`, missing values will be removed prior to uniting each value.
#' @export
setunite <- function(data, col, ..., sep = "_", remove = TRUE, na.rm = FALSE) {
  . <- NULL

  assert_data_table(data)

  col <- rlang::as_name(col)
  cols <- colnames(data)[tidyselect::eval_select(rlang::expr(c(...)),data)]

  united <- tidyr::unite(data[,cols,with=F], col, dplyr::all_of(cols), sep = sep, remove = TRUE, na.rm = na.rm) %>%
    setDT %>% .[,col]

  data[, (col) := united]

  cols_to_remove <- setdiff(cols,col)
  if(remove && length(cols_to_remove) > 0)
    data[, (cols_to_remove) := NULL]

}

#' stream_customer_history
#'
#' Loads the last row from `stream` after sorting by `timestamp` and grouping by the entries in columns `by`,
#' but only looking at timestamps before `before` and returning only columns matching `pattern`
#'
#' @param stream data.frameish stream
#' @param pattern character vector. If length > 1, the union of the matches is taken.
#' @inheritDotParams tidyselect::matches ignore.case perl
#' @param before POSIXct only look at customer history before this date
#' @param by character column name to group by
#' @param ...
#'
#' @importFrom tidyselect matches
#' @importFrom data.table setorderv
#' @importFrom dplyr filter select collect all_of semi_join
#' @importFrom checkmate assert_names
stream_customer_history <- function(stream, by, before = as.POSIXct("2100-01-01"), pattern = ".", ...) {
  timestamp <- NULL
  assert_names(names(stream), must.include = c("timestamp", by))

  stream <- stream %>% filter(timestamp < before) %>% 
    select(all_of(c(by,"timestamp")),matches(pattern,...))
  
  if (is.null(stream$timestamp_id)) {
    if (inherits(stream, c("ArrowTabular", "arrow_dplyr_query"))) {
      stream <- stream %>% mutate(timestamp_id = arrow:::cast(timestamp, arrow::int64()))
    } else {
      stream <- stream %>% mutate(timestamp_id = timestamp)
    }
  }

  stream_dates <- stream %>%
    select(all_of(c(by,"timestamp", "timestamp_id"))) %>%
    # have to pull this into R in order to do windowed slices, i.e. debouncing
    collect %>% setDT %>%
    setkeyv(c(by, "timestamp_id", "timestamp")) %>%
    stream_debounce(by)
  
  if(nrow(stream_dates) == 0)
    return(arrow::arrow_table(schema = arrow::schema(stream)) %>% 
             collect %>% setDT)
    
  if (inherits(stream, c("ArrowTabular","arrow_dplyr_query")))
    stream_dates <- arrow::as_arrow_table(stream_dates)
  
  stream  %>%
    semi_join(stream_dates, by=c(by, "timestamp_id")) %>%
    select(-timestamp_id) %>% 
    collect %>% setDT

}

