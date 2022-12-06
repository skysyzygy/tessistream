# load_addresses_audit
# load_addresses
# build_address_stream
# libpostal_clean
# census_geocode
# google_geocode
# census_reverse_geocode
# census_append
# iwave_append
# Sys.setenv("TAR_PROJECT"="address_stream") # nolint

address_cols = c("street1"="street1",
                 "street2"="street2",
                 "city"="city",
                 "state_desc"="state",
                 "postal_code"="postal_code",
                 "country_desc"="country")

#' @importFrom dplyr transmute select filter coalesce
#' @importFrom data.table setDT dcast
#' @importFrom tessilake read_tessi
address_load_audit <- function(freshness) {
  table_name <- column_updated <- group_customer_no <- new_value <- old_value <- alternate_key <- userid <- NULL

  # Address changes
  aa = read_tessi("audit", freshness = freshness) %>%
    filter(table_name=="T_ADDRESS" & column_updated %in% c(address_cols,"primary_ind")) %>%
    transmute(group_customer_no,
              timestamp=date,
              new_value=coalesce(new_value,""),
              old_value=coalesce(old_value,""),
              address_no=as.integer(as.character(alternate_key)),
              last_updated_by=userid,
              column_updated,old_value,new_value) %>%
    collect() %>% setDT
}

#' @importFrom dplyr transmute select filter
#' @importFrom data.table setDT setnames
#' @importFrom tessilake read_tessi
address_load <- function(freshness) {
  . <- group_customer_no <- create_dt <- address_no <- created_by <- last_update_dt <- last_updated_by <- NULL

  a = read_tessi("addresses", freshness = freshness) %>% collect %>% setDT

  cols = c(address_cols,primary_ind="primary_ind")

  rbind(
    # Address creations
    a[,.(group_customer_no,event_type="Address",event_subtype="Creation",
         timestamp=create_dt,
         address_no,
         last_updated_by=created_by)],
    # Address last modifications
    cbind(a[,.(group_customer_no,event_type="Address",event_subtype="Current",
         timestamp=last_update_dt,
         address_no,
         last_updated_by)],
         a[,names(cols),with=F] %>%
           setnames(names(cols),cols)),
    fill = T
  )
}


#' address_create_stream
#'
#' Creates address data with timestamps from TA_AUDIT_TABLE and T_ADDRESS data
#'
#' @param freshness data will be at least this fresh
#'
#' @return data.table of addresses data at different points of time, no more than one address per customer per day
#'
#' @importFrom tessilake read_tessi read_sql_table
#' @importFrom dplyr collect transmute filter select
#' @importFrom data.table setDT setkey
address_create_stream <- function(freshness = as.difftime(7,units="days")) {
  . <- address_no <- timestamp <- NULL

  aa = address_load_audit(freshness)

  address_audit_stream = aa %>%
    dcast(group_customer_no + timestamp + address_no + last_updated_by ~ column_updated, value.var="new_value") %>%
    .[,`:=`(event_type="Address",event_subtype="Change")]

  address_audit_creation = aa %>% .[,.SD[1],by=c("address_no","column_updated")] %>%
    dcast(address_no ~ column_updated, value.var="old_value") %>%
    .[,`:=`(event_type="Address",event_subtype="Creation")]

  address_stream = rbind(address_load(freshness), address_audit_stream, fill = TRUE)

  cols <- c(address_cols,"primary_ind")

  # Address creation fill-up based on audit old_value -- all other old_values are captured within the audit table itself
  address_stream[address_audit_creation,
                 (cols):=mget(paste0("i.",cols),ifnotfound = NA),
                 on=c("address_no","event_subtype")]

  setkey(address_stream,address_no,timestamp)

  address_fill_debounce_stream(address_stream)
}

address_fill_debounce_stream <- function(address_stream) {
  event_subtype <- address_no <- timestamp <- group_customer_no <- NULL
  cols <- c(address_cols,"primary_ind")

  # Factorize event_subtype
  address_stream[,event_subtype:=factor(event_subtype,levels=c("Creation","Change","Current"))]

  setkey(address_stream,address_no,event_subtype,timestamp)
  # Fill-down changes
  setnafill(address_stream,"locf",cols = cols, by = "address_no")
  # And then fill back up for non-changes
  setnafill(address_stream,"nocb",cols = cols, by = "address_no")

  stream_debounce(address_stream,group_customer_no,address_no)
}

# Street cleaning ---------------------------------------------------------

#' address_clean
#'
#' Removes newlines, tabs, lowercases, trims whitespace, and removes junk info
#'
#' @param address_stream data.table of addresses
#'
#' @return data.table of addresses cleaned
#' @importFrom purrr map
#' @importFrom stringr str_replace_all
address_clean <- function(address_stream) {
  street1 <- city <- state <- postal_code <- NULL

  # Remove newlines, tabs, etc.
  address_stream[,(address_cols):=map(.SD,~str_replace_all(.,"\\s+"," ")),.SDcols=address_cols]

  # Lowercase and trim whitespace from address fields
  address_stream[,(address_cols):=lapply(.SD,function(.) {trimws(tolower(.))}),.SDcols=address_cols]

  # remove junk info
  lapply(address_cols,function(col){
        address_stream[grepl("^(web add|unknown|no add)|^$",get(col)),(col):=NA_character_]
  })

  address_stream = address_stream[!(grepl("^30 lafayette",street1) & (city=="brooklyn" & state=="ny" | substr(postal_code,1,5)=="11217"))]

}

#' address_exec_libpostal
#'
#' @param addresses character vector of addresses
#'
#' @return data.frame of parsed addresses, one row per vector i
#'
#' @importFrom checkmate assert_directory_exists assert_character
#' @importFrom jsonlite fromJSON
#' @importFrom utils tail
#' @describeIn address_parse execute address_parser and query it with a vector of addresses
address_exec_libpostal <- function(addresses) {
  . <- NULL

  assert_character(addresses,any.missing=FALSE,min.len=1)
  libpostal <- Sys.which("address_parser")
  if(libpostal=="")
    stop("libpostal address_parser executable not found, add to PATH")

  ret = withr::with_dir(tempdir(),{
    # Encode UTF-8
    addresses = enc2utf8(addresses)
    # and stop writeLines from re-encoding
    Encoding(addresses) <- "bytes"
    ret = system2(libpostal,stdout=T,input=addresses)
  })


  ret = iconv(ret,from="utf-8") %>% tail(-match("Result:",.)-1)
  ret[which(ret=="Result:")]=","

  fromJSON(c("[",ret,"]"))
}


#' address_parse_libpostal
#'
#' @param address_stream data.table of addresses
#'
#' @return data.table of addresses parsed, one per unique address in address_stream
#' @importFrom tidyr unite
#' @importFrom stringr str_detect str_match str_remove fixed
#' @importFrom dplyr any_of distinct
#' @describeIn address_parse handle parsing by libpostal
address_parse_libpostal <- function(address_stream) {
  address <- unit <- postcode <- street2 <- road <- NULL

  assert_data_table(address_stream)

  # one row per unique address
  address_stream <- address_stream[,..address_cols] %>% distinct %>% setDT

  # make address string for libpostal
  address_stream[,address:=unite(.SD,"address",sep=", ",na.rm=T),.SDcols=address_cols]
  addresses <- tolower(address_stream[!is.na(address),address])

  #TODO: map english numbers to numerals
  parsed <- data.table(address=addresses,address_exec_libpostal(addresses))
  parsed <- parsed[address_stream$address,on="address"]
  parsed[,I:=.I]

  # add columns if they don't exist
  parsed <- rbind(parsed,data.table(address=character(0),unit=character(0),postcode=character(0),road=character(0)),fill = TRUE)
  address_stream <- rbind(address_stream,data.table(street2=character(0)), fill = TRUE)

  streets_regex = tolower("(ALLEY|ALLEE|ALY|ALLY|ANEX|ANX|ANNEX|ANNX|ARCADE|ARC|AVENUE|AV|AVE|AVEN|AVENU|AVN|AVNUE|BAYOU|BAYOO|BYU|BEACH|BCH|BEND|BND|BLUFF|BLF|BLUF|BLUFFS|BLFS|BOTTOM|BOT|BTM|BOTTM|BOULEVARD|BLVD|BOUL|BOULV|BRANCH|BR|BRNCH|BRIDGE|BRDGE|BRG|BROOK|BRK|BROOKS|BRKS|BURG|BG|BURGS|BGS|BYPASS|BYP|BYPA|BYPAS|BYPS|CAMP|CP|CMP|CANYON|CANYN|CYN|CNYN|CAPE|CPE|CAUSEWAY|CSWY|CAUSWA|CENTER|CEN|CTR|CENT|CENTR|CENTRE|CNTER|CNTR|CENTERS|CTRS|CIRCLE|CIR|CIRC|CIRCL|CRCL|CRCLE|CIRCLES|CIRS|CLIFF|CLF|CLIFFS|CLFS|CLUB|CLB|COMMON|CMN|COMMONS|CMNS|CORNER|COR|CORNERS|CORS|COURSE|CRSE|COURT|CT|COURTS|CTS|COVE|CV|COVES|CVS|CREEK|CRK|CRESCENT|CRES|CRSENT|CRSNT|CREST|CRST|CROSSING|XING|CRSSNG|CROSSROAD|XRD|CROSSROADS|XRDS|CURVE|CURV|DALE|DL|DAM|DM|DIVIDE|DIV|DV|DVD|DRIVE|DR|DRIV|DRV|DRIVES|DRS|ESTATE|EST|ESTATES|ESTS|EXPRESSWAY|EXP|EXPY|EXPR|EXPRESS|EXPW|EXTENSION|EXT|EXTN|EXTNSN|EXTENSIONS|EXTS|FALL|FALLS|FLS|FERRY|FRY|FRRY|FIELD|FLD|FIELDS|FLDS|FLAT|FLT|FLATS|FLTS|FORD|FRD|FORDS|FRDS|FOREST|FRST|FORESTS|FORGE|FORG|FRG|FORGES|FRGS|FORK|FRK|FORKS|FRKS|FORT|FT|FRT|FREEWAY|FWY|FREEWY|FRWAY|FRWY|GARDEN|GDN|GARDN|GRDEN|GRDN|GARDENS|GDNS|GRDNS|GATEWAY|GTWY|GATEWY|GATWAY|GTWAY|GLEN|GLN|GLENS|GLNS|GREEN|GRN|GREENS|GRNS|GROVE|GROV|GRV|GROVES|GRVS|HARBOR|HARB|HBR|HARBR|HRBOR|HARBORS|HBRS|HAVEN|HVN|HEIGHTS|HT|HTS|HIGHWAY|HWY|HIGHWY|HIWAY|HIWY|HWAY|HILL|HL|HILLS|HLS|HOLLOW|HLLW|HOLW|HOLLOWS|HOLWS|INLET|INLT|ISLAND|IS|ISLND|ISLANDS|ISS|ISLNDS|ISLE|ISLES|JUNCTION|JCT|JCTION|JCTN|JUNCTN|JUNCTON|JUNCTIONS|JCTNS|JCTS|KEY|KY|KEYS|KYS|KNOLL|KNL|KNOL|KNOLLS|KNLS|LAKE|LK|LAKES|LKS|LAND|LANDING|LNDG|LNDNG|LANE|LN|LIGHT|LGT|LIGHTS|LGTS|LOAF|LF|LOCK|LCK|LOCKS|LCKS|LODGE|LDG|LDGE|LODG|LOOP|LOOPS|MALL|MANOR|MNR|MANORS|MNRS|MEADOW|MDW|MEADOWS|MDWS|MEDOWS|MEWS|MILL|ML|MILLS|MLS|MISSION|MISSN|MSN|MSSN|MOTORWAY|MTWY|MOUNT|MNT|MT|MOUNTAIN|MNTAIN|MTN|MNTN|MOUNTIN|MTIN|MOUNTAINS|MNTNS|MTNS|NECK|NCK|ORCHARD|ORCH|ORCHRD|OVAL|OVL|OVERPASS|OPAS|PARK|PRK|PARKS|PARKWAY|PKWY|PARKWY|PKWAY|PKY|PARKWAYS|PKWYS|PASS|PASSAGE|PSGE|PATH|PATHS|PIKE|PIKES|PINE|PNE|PINES|PNES|PLACE|PL|PLAIN|PLN|PLAINS|PLNS|PLAZA|PLZ|PLZA|POINT|PT|POINTS|PTS|PORT|PRT|PORTS|PRTS|PRAIRIE|PR|PRR|RADIAL|RAD|RADL|RADIEL|RAMP|RANCH|RNCH|RANCHES|RNCHS|RAPID|RPD|RAPIDS|RPDS|REST|RST|RIDGE|RDG|RDGE|RIDGES|RDGS|RIVER|RIV|RVR|RIVR|ROAD|RD|ROADS|RDS|ROUTE|RTE|ROW|RUE|RUN|SHOAL|SHL|SHOALS|SHLS|SHORE|SHOAR|SHR|SHORES|SHOARS|SHRS|SKYWAY|SKWY|SPRING|SPG|SPNG|SPRNG|SPRINGS|SPGS|SPNGS|SPRNGS|SPUR|SPURS|SQUARE|SQ|SQR|SQRE|SQU|SQUARES|SQRS|SQS|STATION|STA|STATN|STN|STRAVENUE|STRA|STRAV|STRAVEN|STRAVN|STRVN|STRVNUE|STREAM|STRM|STREME|STREET|ST|STRT|STR|STREETS|STS|SUMMIT|SMT|SUMIT|SUMITT|TERRACE|TER|TERR|THROUGHWAY|TRWY|TRACE|TRCE|TRACES|TRACK|TRAK|TRACKS|TRK|TRKS|TRAFFICWAY|TRFY|TRAIL|TRL|TRAILS|TRLS|TRAILER|TRLR|TRLRS|TUNNEL|TUNEL|TUNL|TUNLS|TUNNELS|TUNNL|TURNPIKE|TRNPK|TPKE|TURNPK|UNDERPASS|UPAS|UNION|UN|UNIONS|UNS|VALLEY|VLY|VALLY|VLLY|VALLEYS|VLYS|VIADUCT|VDCT|VIA|VIADCT|VIEW|VW|VIEWS|VWS|VILLAGE|VILL|VLG|VILLAG|VILLG|VILLIAGE|VILLAGES|VLGS|VILLE|VL|VISTA|VIS|VIST|VST|VSTA|WALK|WALKS|WALL|WAY|WY|WAYS|WELL|WL|WELLS|WLS)")
  directions_regex = tolower("(N|NE|NW|S|SE|SW|E|W|NORTE|NORESTE|NOROESTE|SUR|SURESTE|SUROESTE|ESTE|OESTE|NORTH|NORTHEAST|NORTHWEST|SOUTH|SOUTHEAST|SOUTHWEST|EAST|WEST)")
  unit_regex = tolower("(Apartment(?!$)|APT(?!$)|Basement|BSMT|Building(?!$)|BLDG(?!$)|Department(?!$)|DEPT(?!$)|Floor|FL|Front|FRNT|Hanger(?!$)|HNGR(?!$)|Key(?!$)|KEY(?!$)|Lobby|LBBY|Lot(?!$)|LOT(?!$)|LOWR|Office|OFC|Penthouse|PH|Pier(?!$)|PIER(?!$)|Rear|REAR|Room(?!$)|RM(?!$)|Slip(?!$)|SLIP(?!$)|Space(?!$)|SPC(?!$)|Stop(?!$)|STOP(?!$)|Suite(?!$)|STE(?!$)|Trailer(?!$)|TRLR(?!$)|Unit(?!$)|UNIT(?!$)|UPPR|#(?!$))")
  unit_number_regex = "(\\d[\\d\\w]*|\\w\\d+|ph.{1,3})"
  unit_regex2 <- paste0("^",unit_regex,"?\\s*",unit_number_regex,"$")
  street_unit_regex <- paste0("\\W+",streets_regex,"\\W+","(",directions_regex,"\\W+",")?","((",unit_regex,"|",unit_number_regex,")")

  # libpostal returns:
  # house_number => house_number
  # po_box => po_box,
  # road => road,
  # unit/level/entrance/staircase => unit,
  # house/category/near => house
  # city/city_district/suburb/island => city
  # state/state_district => state
  # postcode => postcode
  # country/country_region/world_region => country

  # But the parsing is imperfect. The hardest to resolve is unit.

  # ... for some reason a lot of units end up in postcode!
  parsed[is.na(unit) & postcode != address_stream$postal_code[I],
                 unit:=postcode]
  # ... some units don't get detected in street2
  parsed[is.na(unit) & str_detect(address_stream$street2[I],unit_regex2),
                 unit:=address_stream$street2[I]]
  # ... and if the road has the unit in it, put it in unit
  parsed[is.na(unit) & str_detect(road,paste0(street_unit_regex,")")),
                 unit:=str_match(road,paste0(street_unit_regex,".*$)"))[,5]]
  # ... finally cleanup unit
  parsed[,unit:=trimws(unit)]
  parsed[unit=="",unit:=NA]
  # And remove it from other fields if it's duplicatred
  lapply(intersect(c("postcode","road","house"),colnames(parsed)),
  function(col){
    parsed[!is.na(unit),(col):=str_remove(get(col),
  # Escape the unit so that it can work as a regex
      paste0("(^|\\W+)",str_replace_all(unit,"[^a-z0-9]","\\$0"),"(\\W+|$)"))]
    parsed[!is.na(unit) & get(col)=="",(col):=NA_character_]
  })

  # Now merge everything else together
  if(any(c("unit","level","entrance","staircase") %in% colnames(parsed)))
    parsed <- parsed %>% unite("unit",any_of(c("unit","level","entrance","staircase")),sep=" ",na.rm=T)
  if(any(c("house","category","near") %in% colnames(parsed)))
    parsed <- parsed %>% unite("house",any_of(c("house","category","near")),sep=" ",na.rm=T)
  if(any(c("suburb","city_district","city","island") %in% colnames(parsed)))
    parsed <- parsed %>% unite("city",any_of(c("suburb","city_district","city","island")),sep=", ",na.rm=T)
  if(any(c("state_district","state") %in% colnames(parsed)))
    parsed <- parsed %>% unite("state",any_of(c("state_district","state")),sep=", ",na.rm=T)
  if(any(c("country_region","country","world_region") %in% colnames(parsed)))
    parsed <- parsed %>% unite("country",any_of(c("country_region","country","world_region")),sep=", ",na.rm=T)

  # ok maybe we're finally done. Let's clean up
  out_cols = intersect(
    c("house_number","road","unit","house","po_box","city","state","country","postcode"),
    colnames(parsed))
  parsed = parsed[,out_cols,with=F]

  lapply(colnames(parsed),
         function(col){
           parsed[get(col)=="",(col):=NA_character_]
         })

  address_stream[,address:=NULL]

  address_stream = cbind(address_stream,libpostal=parsed)

}

#' address_parse
#'
#' Parses addresses using libpostal and handles caching of already-parsed addresses so that they're only parsed once
#'
#' @param address_stream data.table of addresses
#'
#' @return data.table of addresses parsed
#' @importFrom dplyr collect
address_parse <- function(address_stream) {
  ..address_cols <- ..columns <- NULL

  assert_data_table(address_stream)

  cache <- tessilake:::cache_read("address_parse","deep","stream")

  if(cache == FALSE) {
    # Cache doesn't yet exist
    address_stream <- cache <- address_parse_libpostal(address_stream)
  } else {
    cache <- collect(cache)
    cache_miss <- address_stream[!cache,..address_cols,on=as.character(address_cols)]
    cache <- rbind(cache,address_parse_libpostal(cache_miss),fill = TRUE)
    address_stream <- cache[address_stream,on=as.character(address_cols)]
  }

  # only save needed columns
  columns <- c(grep("^libpostal",colnames(cache),value = TRUE),address_cols)
  tessilake:::cache_write(cache[,..columns],"address_parse","deep","stream",overwrite = TRUE)

  return(address_stream)
}

# Geocoding ---------------------------------------------------------------



if(FALSE)  {








  # Add geocode info from the zipcodeR database

zip_code_db = as.data.table(zip_code_db)
BAM_center = c(-73.977765,40.686876)

# But not every postal code has a centroid so we need to do some fancy re-matching
# First clean the postal codes so they are all five digits and make a 2-digit blocking criteria
addressStream[!is.na(postal_code) & country=="usa" & stringr::str_length(postal_code)>4 &
                grepl("^\\d+$",postal_code) & as.numeric(postal_code)>0,
                                              .(zipcode=substr(postal_code,1,5),
                                                 zipcode2=substr(postal_code,1,2))] %>% distinct %>%
  merge(zip_code_db[!is.na(lat) & !is.na(lng),.(zipcode,lat,lng,
                                                zipcode2=substr(zipcode,1,2))],
        by="zipcode2",all.x=T,allow.cartesian=T) %>% .[!is.na(lat) & !is.na(lng)] %>%
# Then join them together using the closest (numerically) postal code within the blocking criteria
  .[,diff:=abs(as.numeric(zipcode.x)-as.numeric(zipcode.y))] %>% setkey(zipcode.x,diff) %>% .[,.SD[1],by="zipcode.x"] %>%
  .[,.(zipcode=zipcode.x,lat,lng)] %>%
# Then merge it all together
  merge(addressStream[,zipcode:=substr(postal_code,1,5)],all.y=T,by="zipcode")  -> addressStream

# 99.9% of valid postal codes now have a geocode
testit::assert(addressStream[stringr::str_length(postal_code)>4 & grepl("^\\d+$",postal_code) & country=="usa"] %>%
  {nrow(.[is.na(lat)])/nrow(.)} < .001)

# remove blanks
addressStream[,colnames(addressStream):=lapply(.SD,function(c){if(is.character(c)){replace(c,which(c==""),NA)} else {c}})]


# Census Geocoding --------------------------------------------------------

# Prepare for census geocoding
benchmark = cxy_benchmarks() %>% filter(grepl("Current",benchmarkName)) %>% .$benchmarkName
# Vintage is the year of census tracts to look at
vintages = cxy_vintages(benchmark) %>% #filter(!grepl("Current",vintageName)) %>%
  mutate(year=stringr::str_extract(vintageName,"\\d+") %>% as.integer) %>%
# associate one vintage to each year
  merge(data.frame(year=seq(min(year(addressStream$timestamp),na.rm=T),max(year(addressStream$timestamp),na.rm=T))),all=T) %>%
  fill(vintageName,.direction = "updown") %>% select(year,vintageName)

load("geocode_db.RData")

addressStream[,street:=unite(.SD,"street",na.rm=T,sep=" "),.SDcols=c("house_number","road")] %>%
  .[street=="",street:=street1]

addressStream[,year:=year(timestamp)]
setleftjoin(addressStream,vintages,by="year")

# only look at addresses that we have a chance of matching with the census matcher.
addressStream[  !is.na(state) & country=="usa" &
                nchar(postal_code)>4 & grepl("^\\d+$",postal_code) & grepl("^[a-z]",city) &
                as.numeric(postal_code)>0 & !is.na(timestamp),
              .(street1,street,city,state,postal_code,vintageName)] %>%
  {rbind(.[,.(street,city,state,postal_code,vintageName)],
         .[,.(street=street1,city,state,postal_code,vintageName)])} %>%
# clean all fields
  .[,colnames(.):=lapply(.SD,function(.){str_replace_all(.,"[^\\w\\-/\\']+"," ") %>% trimws})] %>%
  .[apply(.,1,function(.){!any(is.na(.) | .=="")})] %>% distinct %>%
# eliminate anything we've already done
  .[!geocode_db,,on=c("street","city","state","postal_code","vintageName")] %>%
# and then associate a cluster if there are a lot to do
  .[,cluster:=rep(seq(.N),length.out=.N,each=100),by="vintageName"] -> for_geocoding


if(nrow(for_geocoding)>0) {
  # geocode the new addresses
  with_progress({
    p = progressor(for_geocoding[,.(cluster,vintageName)] %>% distinct %>% nrow)

    # Split up addresses for parallel processing
    for_geocoding %>% group_by(vintageName,cluster) %>% group_split %>% split(rep(seq(.),
                                                                                  length.out=length(.),
                                                                                  each=100)) %>%
    # Outer loop saves periodically
      lapply(function(.y) {
        # Inner loop creates futures for parallel processing
        lapply(.y,function(.x) {
          future({
            ret = cxy_geocode(.x,street = "street",city = "city",state = "state",zip = "postal_code",vintage = .x$vintageName[[1]],
                              return = "geographies",benchmark = benchmark,output = "full",class = "dataframe")
            p();ret
          })
        }) %>% value %>% rbindlist %>% rbind(geocode_db,fill=T) ->> geocode_db
        save(geocode_db,file="geocode_db.RData")
      })
  })
}

setDT(vintages)

# Add geocoding to addressStream
geocode = geocode_db[!is.na(street)][
  addressStream[,lapply(.SD,function(.){str_replace_all(.,"[^\\w\\-/\\']+"," ") %>% trimws}),
                .SDcols=c("street","city","state","postal_code","vintageName")],
  on=c("street","city","state","postal_code","vintageName")][,I:=.I]

street1_geocode = geocode_db[!is.na(street)][
  addressStream[,lapply(.SD,function(.){str_replace_all(.,"[^\\w\\-/\\']+"," ") %>% trimws}),
                .SDcols=c("street1","city","state","postal_code","vintageName")],
  on=c("street"="street1","city","state","postal_code","vintageName")]

geocode[(cxy_quality!="Exact" | is.na(cxy_quality)) & street1_geocode$cxy_quality=="Exact",
        colnames(street1_geocode):=street1_geocode[I]]

# TEST: Address stream and geocode are the same length
testit::assert(nrow(addressStream)==nrow(geocode))

addressStream = cbind(addressStream,geocode[,setdiff(colnames(street1_geocode),
                                                     colnames(addressStream)),with=F])

rm(geocode,street1_geocode)

# Google Geocoding --------------------------------------------------------

load("google_geocode_db.RData")

# send all the rest to the Google matcher
addressStream[cxy_quality!="Exact" | is.na(cxy_quality),
            .(street1,street,city,state,postal_code,country,timestamp)] %>%
  {rbind(.[,.(street,city,state,postal_code,country,timestamp)],
         .[,.(street=street1,city,state,postal_code,country,timestamp)])} %>%
  # clean all fields
  .[,colnames(.):=lapply(.SD,function(.){if(is.character(.))
                      {str_replace_all(.,"[^\\w\\-/\\']+"," ") %>% trimws} else {.}})] %>%
  .[apply(.,1,function(.){!all(is.na(.) | .=="")})] %>%
  .[,timestamp:=max(timestamp),by=c("street","city","state","postal_code","country")] %>% distinct %>%
  # eliminate anything we've already done
  .[!google_geocode_db,,on=c("street","city","state","postal_code","country")] %>%
  setorder(-timestamp) %>%
  # But only do up to 40,000 per month, or 1,500 per day, whichever is less
  .[1:min(.N,1500,40000-google_geocode_db[google.timestamp>today()-months(1),.N])] %>%
  # and then associate a cluster if there are a lot to do
  .[,cluster:=rep(seq(.N),length.out=.N,each=100)] -> for_geocoding

if(nrow(for_geocoding)>0) {
  # geocode the new addresses
  with_progress({
    p = progressor(for_geocoding[,cluster] %>% unique %>% length)
    register_google(keyring::key_get("google_geocode_api"))

    # Split up addresses for progress -- but don't do parallel processing because Google API has stict per-second limit!
    for_geocoding %>% group_by(cluster) %>% group_split %>%
      lapply(function(.x) {
          ret = geocode(setDT(.x)[,unite(.SD,"location",na.rm=T,sep=", ")[[1]],.SDcols=
                                        c("street","city","state","postal_code","country")],
                        output="all")
          p();ret
      }) %>% value -> google_temp
  })
}

google_temp %>% do.call(what="c") %>% map(~tryCatch({
  c(status=.$status,
    .$results[[1]]$address_components %>% {setNames(map(.,"short_name"),
                                                    map(.,~.$types[[1]]))},
    .$results[[1]]$geometry$location,
    location_type=.$results[[1]]$geometry$location_type,
    type=.$results[[1]]$types[[1]],
    timestamp=now()) },error=function(e){list(NA)})) %>%
  rbindlist(fill=T) %>% {cbind(for_geocoding,google=.)} %>% rbind(google_geocode_db,fill=T) ->> google_geocode_db

save(google_geocode_db,file="google_geocode_db.RData")

# Add geocoding to addressStream
geocode = google_geocode_db[
  addressStream[,lapply(.SD,function(.){str_replace_all(.,"[^\\w\\-/\\']+"," ") %>% trimws}),
                .SDcols=c("street","city","state","postal_code","country","cxy_quality")],,
  on=c("street","city","state","postal_code","country")][,I:=.I]

street1_geocode = google_geocode_db[
  addressStream[,lapply(.SD,function(.){str_replace_all(.,"[^\\w\\-/\\']+"," ") %>% trimws}),
                .SDcols=c("street1","city","state","postal_code","country")],,
  on=c("street"="street1","city","state","postal_code","country")]

geocode[(cxy_quality!="Exact" | is.na(cxy_quality)) &
        (google.status!="OK" | is.na(google.status)) & street1_geocode$google.status=="OK",
        colnames(street1_geocode):=street1_geocode[I]]

# TEST: Address stream and geocode are the same length
testit::assert(nrow(addressStream)==nrow(geocode))

addressStream = cbind(addressStream,geocode[,setdiff(colnames(street1_geocode),
                                                     colnames(addressStream)),with=F])

# TEST: Address information matches associated geocode
testit::assert(addressStream[!str_detect(str_replace_all(cxy_address,"[^a-z0-9\\-/\\']+"," ") %>% trimws,
                                         fixed(str_replace_all(street,"[^a-z0-9\\-/\\']+"," ") %>% trimws)) &
                             !str_detect(str_replace_all(cxy_address,"[^a-z0-9\\-/\\']+"," ") %>% trimws,
                                         fixed(str_replace_all(street1,"[^a-z0-9\\-/\\']+"," ") %>% trimws)) &
                               !is.na(cxy_address),.N]<10)

rm(geocode,street1_geocode)

# TEST: 85% of valid postal codes now have a geocode
testit::assert(addressStream[!is.na(state) & country=="usa" &
                               nchar(postal_code)>4 & grepl("^\\d+$",postal_code) & grepl("^[a-z]",city) &
                               as.numeric(postal_code)>0 & !is.na(timestamp)] %>%
                 {nrow(.[is.na(cxy_lat)])/nrow(.)} < .15)

addressStream[,`:=`(lat=coalesce(cxy_lat,google.lat,lat),
                    lng=coalesce(cxy_lon,google.lng,lng),I=.I)]


# Reverse geocoding -------------------------------------------------------

# Add geoid data (tract/block/county/state) to rows that are missing it
addressStream[is.na(cxy_tract_id) & !is.na(lat) & !is.na(lng) & (country=="usa" | google.country=="usa"),
              .(cxy_lon=lng,cxy_lat=lat,benchmark,vintageName)] %>% distinct %>%
  # eliminate anything we've already done
  .[!geocode_db,,on=c("cxy_lon","cxy_lat","vintageName")] %>%
  # and then associate a cluster if there are a lot to do
  .[,cluster:=rep(seq(.N),length.out=.N,each=100)] -> for_geocoding

with_progress({
  p = progressor(n_distinct(for_geocoding$cluster))
  for_geocoding %>% group_split(cluster) %>%
    lapply(function(.x){
      future({
      ret = lapply(transpose(.x),function(.){
          cxy_geography(.[1],.[2],.[3],vintage=coalesce(.[4],"Current_Current"))
        }) %>% rbindlist(.,fill=T)
      p(); ret
      })
    }) %>% value -> geocode_temp
})

geocode_temp %>% rbindlist(fill=T) %>% .[,.(cxy_state_id=States.STATE,
                                            cxy_county_id=Counties.COUNTY,
                                            cxy_tract_id=Census.Tracts.TRACT,
                                            cxy_block_id=Census.Blocks.BLOCK)] %>% cbind(for_geocoding[,-c("benchmark")]) %>%
  rbind(geocode_db,fill=T) -> geocode_db
save(geocode_db,file="geocode_db.RData")

addressStream[
  geocode_db[addressStream[is.na(cxy_tract_id) & !is.na(lat) & !is.na(lng) & (country=="usa" | google.country=="usa"),
             .(cxy_lon=lng,cxy_lat=lat,benchmark,vintageName,I)],
             .(cxy_state_id,cxy_county_id,cxy_tract_id,cxy_block_id,I),
             on=c("cxy_lon","cxy_lat","vintageName")],
             `:=`(cxy_state_id=i.cxy_state_id,
                  cxy_county_id=i.cxy_county_id,
                  cxy_tract_id=i.cxy_tract_id,
                  cxy_block_id=i.cxy_block_id),on="I"] -> addressStream

# 99.9% of geocoded US addresses have census tract info
testit::assert(addressStream[!is.na(lat) & !is.na(lng) & (country=="usa" | google.country=="usa")] %>%
                 {.[is.na(cxy_tract_id),.N]/.[,.N]} < .001)

# Add distance and bearing info to addressStream
with_progress({
  p = progressor(100)
  addressStream[!is.na(lat) & !is.na(lng),.(lat,lng,I,cluster=rep(seq(100),length.out=.N))] %>% group_split(cluster) %>%
    lapply(function(.) {
      future({
        ret = setDT(.)[,`:=`(distance=distm(cbind(lng,lat),BAM_center,distCosine),
                      bearing=bearingRhumb(BAM_center,cbind(lng,lat)))]
        p(); ret
      },packages = c("geosphere"))
    })
}) %>% value %>% rbindlist %>% .[addressStream,c(colnames(addressStream),c("distance","bearing")),on="I",with=F] -> addressStream


# Census data -------------------------------------------------------------

#census_api_key("6d0fd688fc79ce5d0f5484785dfc4b3717801436",install=T,overwrite=T)

# Get variable names for ACS 5-year surveys
variables = lapply(seq(2009,2019),function(year) {
  load_variables(year,"acs5/profile",cache=T) %>% mutate(year=year) %>%
    {rbind(
      filter(.,grepl("^(number|estimate).+sex and age.+(to |85)",label,ignore.case=T)) %>% mutate(type="sex_and_age"),
      filter(.,grepl("^(number|estimate).+one race!![^!]+$",label,ignore.case=T)) %>% mutate(type="race"),
      filter(.,grepl("^(number|estimate).+(median|mean) household income",label,ignore.case=T)) %>% mutate(type="income")
    )}
  }) %>% rbindlist() %>%
  # and the 2000 decennial survey
  rbind(rbind(load_variables(2000,"sf1",cache=T) %>% mutate(sumfile="sf1",year=2000),
              load_variables(2000,"sf3",cache=T) %>% mutate(sumfile="sf3",year=2000)) %>% {
    rbind(
      filter(.,grepl("Median age!!Both sexes",label,ignore.case=T) & grepl("^MEDIAN.+X \\[",concept)) %>% mutate(type="sex_and_age"),
      filter(.,grepl("one.+race!![^!]+$",label,ignore.case=T) & grepl("^RACE \\[",concept,perl=T)) %>% mutate(type="race"),
      filter(.,grepl("(median|mean) household income",label,ignore.case=T) & grepl("DOLLARS) \\[",concept)) %>% mutate(type="income")
    )},fill=T) %>% mutate(label=stringr::str_extract(label,"[^!]+$")) %>% select(-concept)

if(file.exists("census_db.RData")) {load("census_db.RData")}

if(!exists("census_db")) {
  data(state)
  with_progress({
    p = progressor(10)
    #tract-level data begins with the 2010 ACS
    lapply(c(2000,seq(2011,2019)),function(year) {
      future({
        if(year==2000) {
          ret = rbind(
            get_decennial(geography = "tract",
                          state=state.abb,
                          year = 2000,
                          sumfile = "sf1",
                          cache_table = T,
                          variables = variables[year==2000 & sumfile=="sf1",name],
                          show_call=T),
            get_decennial(geography = "tract",
                          state=state.abb,
                          year = 2000,
                          sumfile = "sf3",
                          cache_table = T,
                          variables = variables[year==2000 & sumfile=="sf3",name],
                          show_call=T))
        } else {
          ret = get_acs(geography = "tract",
                        state=state.abb,
                  year = year,
                  cache_table = T,
                  variables = variables[year==year,name],
                  show_call=T)
        }
        p()
        mutate(ret,year=year)
      },packages = c("data.table"))
    })
  }) %>% value %>% rbindlist(fill=T) -> census_db

  # add column names
  merge(census_db,variables,by.x=c("variable","year"),by.y=c("name","year"),all.x=T) %>% .[!is.na(label)] %>%
    .[,.(year,GEOID,estimate=coalesce(estimate,value),label,type)] %>%
  # cleanup and normalize columns
    .[type=="income" & grepl("Median",label),variable:="addressMedianIncomeLevel"] %>%
  # drop mean income for now...
    .[type!="income" | grepl("Median",label)] %>%
  # cleanup race labels
    .[type=="race",variable:=paste0("address",gsub(" |Alone","",stringr::str_to_title(label)),"Level")]  %>%
  # add population totals
    rbind(.[type=="race",.(type=min(type),estimate=sum(estimate,na.rm=T),label=NA,variable="addressTotalLevel"),by=c("GEOID","year")]) %>%
    {rbind(
  # get median age from age brackets
      .[type=="sex_and_age" & year>2000,.(age=stringr::str_extract(label,"\\d+") %>% as.integer() + 5,GEOID,year,estimate)] %>%
          setkey(year,GEOID,age) %>%
          .[,pct:=cumsum(estimate)/sum(estimate,na.rm=T),by=c("year","GEOID")] %>%
          .[pct>.5,.(estimate=age[1],type="sex_and_age"),by=c("year","GEOID")],
      .[type!="sex_and_age" | year==2000],fill=T) %>%
      .[type=="sex_and_age",variable:="addressMedianAgeLevel"]
    } -> census_db

  save(census_db,file="census_db.RData")
}

zcta_crosswalk = as.data.table(zcta_crosswalk)
census_db_zcta = merge(census_db,zcta_crosswalk[,.(GEOID=as.character(GEOID),ZCTA5)],by="GEOID",all.x=T,allow.cartesian = T) %>%
  .[,.(mean=mean(estimate,na.rm=T),sum=sum(estimate,na.rm=T)),by=c("ZCTA5","year","variable")] %>%
  .[,.(ZCTA5,year,variable,estimate=if_else(grepl("Median",variable),mean,sum))] %>%
  .[is.nan(estimate),estimate:=NA]

dcast(census_db,year+GEOID~variable,value.var = "estimate") -> census_db_wide
dcast(census_db_zcta,year+ZCTA5~variable,value.var = "estimate")[!is.na(ZCTA5)] -> census_db_zcta_wide

# Combine with addressStream by ZCTA or zip

addressStream[!is.na(cxy_tract_id),GEOID:=sprintf("%02d%03d%06d",as.integer(cxy_state_id),as.integer(cxy_county_id),as.integer(cxy_tract_id))]
addressStream = census_db_zcta_wide[
  census_db_wide[addressStream,,on=c("GEOID","year"),roll="nearest"],,
                                  on=c("ZCTA5"="zipcode","year"),roll="nearest"]

cols = grep("^i\\.",colnames(addressStream),value=T) %>% sub(pattern="^i\\.",replacement="")
for(col in cols) {
  addressStream[!is.na(get(paste0("i.",col))),(col):=get(paste0("i.",col))]
  addressStream[,(paste0("i.",col)):=NULL]
}


# Output ------------------------------------------------------------------

# make distance and bearing features
addressStream[,c("addressDistanceLevel","addressBearingLevel"):=mget(c("distance","bearing"))][,c("distance","bearing"):=NULL]

# convert demographic data to percentages
demography = grep("^address(?!Median|_|Total|Distance|Bearing|$)",colnames(addressStream),value=T,perl=T)
medians = grep("^addressMedian",colnames(addressStream),value=T)
addressStream[addressTotalLevel>0,(demography):=lapply(.SD,function(x){x*1.0/get("addressTotalLevel")}),.SDcols=demography]

# replace 0 medians and total with NA
addressStream[,(c(medians,"addressTotalLevel")):=lapply(.SD,function(.){if_else(.==0,NA_real_,as.double(.))}),
              .SDcols=c(medians,"addressTotalLevel")]

# update values for customers who move
setkey(addressStream,group_customer_no,timestamp)
addressStream[,I:=1:.N,by=c("group_customer_no")]

for(i in seq(2,addressStream[,max(I)])) {
  # essentially a rolling log average but taking into account NAs
  addressStream[I==i-1 & group_customer_no==lead(group_customer_no) | I==i & group_customer_no==lag(group_customer_no),
                (medians):=lapply(.SD,function(x){
                                  i0 = log(x[seq(1,length(x),2)]+.01)
                                  i1 = log(x[seq(2,length(x),2)]+.01)
                                  i1 = coalesce(i1,i0)
                                  i0 = coalesce(i0,i1)
                                  i1 = exp((i1+i0)/2)
                                  x[seq(2,.N,2)] = i1
                                  x
                                }),.SDcols=medians]
  # bayesian updating of percentages
  addressStream[I==i-1 & group_customer_no==lead(group_customer_no) | I==i & group_customer_no==lag(group_customer_no),
                (demography):=lapply(.SD,function(x){
                  i0 = x[seq(1,length(x),2)]
                  i1 = x[seq(2,length(x),2)]
                  i1 = coalesce(i1,i0)
                  i0 = coalesce(i0,i1)
                  i1 = (i1+i0)/2
                  #i1 = coalesce(i1*i0/(i1*i0+(1-i1)*(1-i0)),i0)
                  x[seq(2,.N,2)] = i1
                  x
                }),.SDcols=demography]
}

addressStream[,I:=NULL]

# Add in iWave data

i = read_tessi("iwave") %>% collect %>% setDT
i[,.(group_customer_no,event_type="Address",event_subtype="iWave Score",
     addressProScoreLevel=pro_score,addressCapacityLevel=capacity_value,
     addressPropertiesLevel=properties_total_value,addressDonationsLevel=donations_total_value,timestamp=floor_date(score_dt,"day"))] %>%
  rbind(addressStream,fill=T) -> addressStream

# Done!

addressStreamFull = copy(addressStream)
addressStream = addressStream[primary_ind=='y' & group_customer_no>=200]

# drop cxy columns
addressStream[,c("cluster","vintageName","GEOID","ZCTA5","year","I","primary_ind",
                 grep("^(cxy|libpostal|google)",colnames(addressStream),value=T)):=NULL]
addressStream[,timestamp:=timestamp_day][,timestamp_day:=NULL]

addressStream = addressStream %>% mutate_all(adaptVmode) %>% as.ffdf
addressStreamFull = addressStreamFull %>% mutate_all(adaptVmode) %>% as.ffdf

pack.ffdf(file.path(streamDir,"addressStream.gz"),addressStream)
pack.ffdf(file.path(streamDir,"addressStreamFull.gz"),addressStreamFull)
rm(list=ls())
save.image()
}
