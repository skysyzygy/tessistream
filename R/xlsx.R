
#' write_xlsx
#'
#' Convenience wrapper around `openxlsx::write.xlsx` to handle some common formatting
#' - row
#' @param data  data to write to the spreadsheet
#' @param xlsx_file  filename to write to
#'
#' @return filename of written xlsx
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom openxlsx addStyle createStyle setColWidths
#' @importFrom purrr map_lgl map_int
#' @importFrom lubridate is.Date
write_xlsx <- function(data, xlsx_file) {
  wb <- createWorkbook()
  addWorksheet(wb, "Sheet 1")

  colnames(data) <- gsub("_"," ",colnames(data)) %>% tools::toTitleCase()

  writeData(wb,1,data,
            rowNames = FALSE,
            headerStyle = createStyle(textDecoration = "bold"))

  setColWidths(wb,1,seq_along(data),pmax(
    map_int(data,
      ~max(c(nchar(as.character(.)),0),na.rm=T)),
    nchar(colnames(data))
  ) + 3)

  addStyle(wb,1,createStyle(halign = "right"),
           rows = 1,
           cols = which(map_lgl(data, ~is.numeric(.) | is.Date(.))),
           stack = TRUE)

  openxlsx::saveWorkbook(wb, xlsx_file)

  xlsx_file
}
