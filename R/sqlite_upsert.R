#' sqlite_upsert
#'
#' Updates a table in an sqlite database based on index columns
#'
#' @param con DBI connection object
#' @param table string name of table
#' @param data data.frame of data to update, rows will be matched on index columns
#' @importFrom dplyr copy_to
#' @importFrom checkmate assert_data_frame assert_class assert_character assert_names
#' @return invisibly
sqlite_upsert <- function(con, table, data) {
  table_name <- column_name <- NULL
  assert_data_frame(data, min.rows = 1)
  assert_class(con, "SQLiteConnection")
  checkmate::assert_choice(table, DBI::dbListTables(con))

  index_cols <- DBI::dbGetQuery(
    con,
    "SELECT m.name table_name, ii.name column_name
    FROM sqlite_schema AS m,
         pragma_index_list(m.name) AS il,
         pragma_index_info(il.name) AS ii
         WHERE m.type='table'"
  ) %>%
    filter(table_name == table) %>%
    select(column_name) %>%
    unlist()

  assert_character(index_cols, min.len = 1, any.missing = F)
  assert_names(names(data), must.include = index_cols)

  missing_cols <- setdiff(names(data), DBI::dbListFields(con, table))
  if (length(missing_cols)) {
    rlang::warn(sprintf("Some columns in data not present in table %s:", table), footer = missing_cols)
  }

  data <- dplyr::select(data, intersect(names(data), DBI::dbListFields(con, table))) %>%
    dplyr::ungroup() %>%
    dplyr::mutate_if(~ !is.double(.) && !is.integer(.), as.character)

  copy_to(con, data, "data", overwrite = TRUE, temporary = TRUE)

  data_names <- paste0("\"", names(data), "\"")

  sql <- paste(
    "INSERT INTO", table, "(", paste(data_names, collapse = ", "), ")",
    "SELECT", paste(data_names, collapse = ", "),
    "FROM data f WHERE true",
    "ON CONFLICT (", paste(index_cols, collapse = ", "), ")",
    "DO UPDATE SET", paste(paste0(data_names, " = excluded.", data_names), collapse = ", ")
  )

  DBI::dbExecute(con, sql)
}
