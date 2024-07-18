
#' address_clean
#'
#' Removes newlines, tabs, lowercases, trims whitespace, and removes junk info
#'
#' @param address_col character vector of address data
#' @param pattern perl-compatible regular expression to use for identifying junk address fields
#'
#' @return data.table of addresses cleaned
#' @importFrom purrr map
#' @importFrom stringr str_replace_all
#' @importFrom checkmate assert_character
address_clean <- function(address_col, pattern = "^(web add|unknown|no add)|^$") {
  assert_character(address_col)

  # Remove newlines, tabs, etc.
  address_col <- str_replace_all(address_col, "\\s+", " ") %>%
    trimws %>% tolower

  address_col[grepl(pattern, address_col, perl = T)] <- NA_character_

  address_col

}


#' address_normalize
#'
#' Normalize addresses using data from [address_parse] and [address_geocode].
#'
#' @param address_stream data.table of addresses
#'
#' @return data.table of addresses parsed, one per row in address_stream. Contains only address_cols and address_cols+`_cleaned`.
#' @importFrom stringr str_replace
#' @importFrom checkmate assert_data_table assert_names
address_normalize <- function(address_stream) {
  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  address_stream <- copy(address_stream)

  # Google doesn't put a comma between the state and the zipcode
  address_stream[grepl("google", query),
              formatted_address := str_replace(formatted_address,
                                             ", ([A-Za-z]+) (\\d+), (\\w+)$",
                                             ", \\1, \\2, \\3")]
  # collect census normalized addresses
  address_stream[grepl("census", query),
                 c("street1_cleaned", "city_cleaned", "state_cleaned", "postal_code_cleaned", "country_cleaned") :=
                 c(tstrsplit(matched_address, split=", "), "USA")]

  # collect google normalized addresses
  address_stream[grepl("google", query) & geometry.location_type == "ROOFTOP" & stringr::str_count(formatted_address, ",") == 4,
                 c("street1_cleaned", "city_cleaned", "state_cleaned",
                 "postal_code_cleaned", "country_cleaned") :=
                 tstrsplit(formatted_address, split=", ")]

  # collect libpostal normalized addresses
  address_stream[is.na(street1_cleaned) & !is.na(libpostal.road),
                `:=`(street1_cleaned = paste(libpostal.house_number,libpostal.road),
                 city_cleaned = libpostal.city,
                 state_cleaned = libpostal.state,
                 postal_code_cleaned = libpostal.postcode,
                 country_cleaned = libpostal.country)]

  # fallback
  address_stream[is.na(street1_cleaned),
                `:=`(street1_cleaned = street1,
                 city_cleaned = city,
                 state_cleaned = state,
                 postal_code_cleaned = postal_code,
                 country_cleaned = country)]


  # fill in street2_cleaned
  address_stream[,street2_cleaned := coalesce(libpostal.po_box, libpostal.unit, libpostal.house)]
  # add unit to street1_cleaned if it wasn't added to street2
  address_stream[!is.na(libpostal.unit) & street2_cleaned != libpostal.unit,
                  street1_cleaned := paste0(street1_cleaned, ", ", libpostal.unit)]

  address_cols_cleaned <- paste0(address_cols,"_cleaned")

  address_stream[, (address_cols_cleaned) :=
  # remove whitespace and known junk values and Title Case
                    map(.SD, ~gsub("\\b(.)","\\U\\1", address_clean(.), perl = T)), .SDcols = address_cols_cleaned]
  # fix state and country case for US
  address_stream[country_cleaned == "Usa", `:=` (state_cleaned = toupper(state_cleaned),
                                                 country_cleaned = toupper(country_cleaned))]

  address_stream[,c(address_cols, address_cols_cleaned), with = F]

}


#' street_cleaner
#'
#' Loads address info for a given list of customers and normalizes it with
#' [address_normalize]
#'
#' @param list_no list number in Tessitura to pull data for
#'
#' @return data.table of cleaned addresses
#' @export
#' @importFrom tessilake read_cache read_sql
#' @importFrom data.table copy
street_cleaner <- function(list_no) {

  list <- read_sql(paste("select * from T_LIST_CONTENTS where list_no=",list_no)) %>%
    collect %>% setDT

  salutations <- read_sql(paste("select * from TX_CUST_SAL where default_ind='Y' and
                          customer_no in (",paste(list$customer_no,collapse=","),")")) %>%
    select(customer_no, esal1_desc, esal2_desc, lsal_desc, business_title) %>%
    collect %>% setDT

  sync_cache("address_stream.sqlite","stream")
  address_stream <- read_cache("address_stream_full","stream") %>%
    filter(primary_ind=="Y" & customer_no %in% list$customer_no) %>% collect %>%
    setDT %>% setkey(customer_no,timestamp)

  address_stream <- address_stream[,last(.SD), by = c("customer_no")]
  address_stream <- cbind(address_stream,address_parse(address_stream))

  addresses_cleaned <- cbind(address_normalize(address_stream),customer_no = address_stream$customer_no)

  addresses_cleaned <- salutations[addresses_cleaned,on="customer_no"]

  addresses_cleaned[sapply(paste(street1_cleaned,street2_cleaned),
                           \(.) {anyDuplicated(strsplit(., " ")[[1]])>0}),
                    repeated_word := TRUE]

  addresses_cleaned[,N:=.N,by=c("street1_cleaned","street2_cleaned","city_cleaned","state_cleaned")] %>%
    .[N>1,possible_duplicate:=TRUE]

  addresses_cleaned
}


