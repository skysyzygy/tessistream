

#' street_cleaner
#' 
#' Loads info from [address_stream] and [address_parse] and cleans it using 
#' information returned from US census/Google datasets to clean/normalize
#' in preparation for mailings.
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
  
  sync_cache("address_stream.sqlite","stream")
  address_stream <- read_cache("address_stream_full","stream") %>% 
    filter(primary_ind=="Y" & customer_no %in% list$customer_no) %>% collect %>% 
    setDT %>% setkey(customer_no,timestamp)
  
  salutations <- read_sql(paste("select * from TX_CUST_SAL where default_ind='Y' and 
                          customer_no in (",paste(list$customer_no,collapse=","),")")) %>%
    select(customer_no, esal1_desc, esal2_desc, lsal_desc, business_title) %>% 
    collect %>% setDT
  
  address_stream <- address_stream[,last(.SD), by = c("customer_no")]
  address_stream <- cbind(address_stream,address_parse(address_stream))
  
  addresses <- copy(address_stream)
  
  # Google doesn't put a comma between the state and the zipcode
  addresses[grepl("google", query),
            formatted_address := stringr::str_replace(formatted_address,
                                                      ", ([A-Za-z]+) (\\d+), (\\w+)$",
                                                      ", \\1, \\2, \\3")]
  # collect census addresses
  addresses[grepl("census", query), 
            c("street1_cleaned", "city_cleaned", "state_cleaned", "postal_code_cleaned", "country_cleaned") :=
              c(tstrsplit(matched_address, split=", "), "USA")]
  
  # collect google addresses
  addresses[grepl("google", query) & geometry.location_type == "ROOFTOP" & stringr::str_count(formatted_address, ",") == 4, 
            c("street1_cleaned", "city_cleaned", "state_cleaned",
              "postal_code_cleaned", "country_cleaned") :=
              tstrsplit(formatted_address, split=", ")]
  
  # collect all the rest
  addresses[is.na(street1_cleaned) & !is.na(libpostal.road), 
            `:=`(street1_cleaned = paste(libpostal.house_number,libpostal.road),
                 city_cleaned = libpostal.city,
                 state_cleaned = libpostal.state,
                 postal_code_cleaned = libpostal.postcode,
                 country_cleaned = libpostal.country)]
  
  addresses[is.na(street1_cleaned), 
            `:=`(street1_cleaned = street1,
                 city_cleaned = city,
                 state_cleaned = state,
                 postal_code_cleaned = postal_code,
                 country_cleaned = country)]
  
  
  # fill in street2
  addresses[,street2_cleaned := stringr::str_replace(coalesce(libpostal.po_box, libpostal.unit, libpostal.house),
                                                     tolower(street1_cleaned),"")]
  addresses[!is.na(libpostal.unit) & street2_cleaned != libpostal.unit, 
            street1_cleaned := paste0(street1_cleaned, ", ", libpostal.unit)]
  
  
  # prepare output
  addresses_cleaned <- addresses[,.(customer_no,
                       street1,street1_cleaned,
                       street2,street2_cleaned,
                       city,city_cleaned,
                       state,state_cleaned,
                       postal_code,postal_code_cleaned,
                       country,country_cleaned,
                       query)] %>% address_clean()
  
  address_cols <- paste(address_cols,"cleaned",sep="_")
  
  # fix case
  addresses_cleaned[, (address_cols) := map(.SD, ~gsub("\\b(.)","\\U\\1", tolower(.), perl = T)),
                    .SDcols = address_cols]
  addresses_cleaned[, `:=` (state = toupper(state_cleaned),
                            country = toupper(country_cleaned))]
  
  addresses_cleaned <- salutations[addresses_cleaned,on="customer_no"]
  
  addresses_cleaned[sapply(paste(street1_cleaned,street2_cleaned), 
                           \(.) {anyDuplicated(strsplit(., " ")[[1]])>0}),
                    repeated_word := TRUE]
  
  addresses_cleaned[,N:=.N,by=c("street1_cleaned","street2_cleaned","city_cleaned","state_cleaned")] %>% 
    .[N>1,possible_duplicate:=TRUE]
}


