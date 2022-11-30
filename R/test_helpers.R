address_prepare_fixtures <- function () {
  . <- N <- table_name <- alternate_key <- address_no <- NULL

  audit <- read_tessi("audit",freshness=0) %>% filter(table_name=="T_ADDRESS") %>% collect %>% setDT %>%
    .[,N:=.N,by=c("group_customer_no","alternate_key")] %>% .[N>10]

  audit[,alternate_key:=as.integer(alternate_key)]

  addresses <- read_tessi("addresses",freshness=0) %>% filter(address_no %in% audit$alternate_key) %>%
    collect %>% setDT

  address_map <- data.table(address_no=unique(c(audit$alternate_key,addresses$address_no)))[,I:=.I]
  addresses <- merge(addresses,address_map,by="address_no") %>% .[,address_no:=I] %>% .[,I:=NULL]
  audit <- merge(audit,address_map,by.x="alternate_key",by.y="address_no") %>% .[,alternate_key:=I] %>% .[,I:=NULL]

  customer_map <- data.table(customer_no=unique(c(audit$customer_no,addresses$customer_no)))[,I:=.I]
  addresses <- merge(addresses,customer_map,by="customer_no") %>% .[,`:=`(customer_no=I,group_customer_no=I)] %>% .[,I:=NULL]
  audit <- merge(audit,customer_map,by="customer_no") %>% .[,`:=`(customer_no=I,group_customer_no=I)] %>% .[,I:=NULL]

  audit[,alternate_key:=as.character(alternate_key)]

  saveRDS(audit,testthat::test_path("address_audit.Rds"))
  saveRDS(addresses,testthat::test_path("addresses.Rds"))

  mockery::stub(address_load, "read_tessi", addresses)
  mockery::stub(address_load_audit, "read_tessi", audit)
  mockery::stub(address_create_stream, "address_load", address_load)
  mockery::stub(address_create_stream, "address_load_audit", address_load_audit)

  stream <- address_create_stream()

  saveRDS(stream,testthat::test_path("address_stream.Rds"))
}
