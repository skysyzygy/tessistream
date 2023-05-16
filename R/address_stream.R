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

address_cols <- c(
  "street1" = "street1",
  "street2" = "street2",
  "city" = "city",
  "state_desc" = "state",
  "postal_code" = "postal_code",
  "country_desc" = "country"
)

#' address_create_stream
#'
#' Creates address data with timestamps from TA_AUDIT_TABLE and T_ADDRESS data
#'
#' @param freshness data will be at least this fresh
#'
#' @return data.table of addresses data at different points of time, no more than one
#' change per address per day
#'
#' @importFrom tessilake read_tessi read_sql_table
#' @importFrom dplyr collect transmute filter select
#' @importFrom data.table setDT setkey
#' @importFrom lubridate as_date
address_create_stream <- function(freshness = as.difftime(7, units = "days")) {
  . <- timestamp <- NULL

  stream_from_audit("addresses", freshness = freshness) %>%
    .[,timestamp := as_date(timestamp)] %>%
    stream_debounce(c("address_no","timestamp"))

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
  address_stream[, (address_cols) := map(.SD, ~ str_replace_all(., "\\s+", " ")), .SDcols = address_cols]

  # Lowercase and trim whitespace from address fields
  address_stream[, (address_cols) := lapply(.SD, function(.) {
    trimws(tolower(.))
  }), .SDcols = address_cols]

  # remove junk info
  lapply(address_cols, function(col) {
    address_stream[grepl("^(web add|unknown|no add)|^$", get(col)), (col) := NA_character_]
  })

  address_stream <- address_stream[!(grepl("^30 lafayette", street1) &
                                       (city == "brooklyn" & state == "ny" | substr(postal_code, 1, 5) == "11217"))]
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

  assert_character(addresses, any.missing = FALSE, min.len = 1)
  libpostal <- Sys.which("address_parser")
  if (libpostal == "") {
    stop("libpostal address_parser executable not found, add to PATH")
  }

  ret <- withr::with_dir(tempdir(), {
    # Encode UTF-8
    addresses <- enc2utf8(addresses)
    # and stop writeLines from re-encoding
    Encoding(addresses) <- "bytes"
    ret <- system2(libpostal, stdout = TRUE, input = addresses)
  })


  ret <- iconv(ret, from = "utf-8") %>% tail(-match("Result:", .) - 1)
  ret[which(ret == "Result:")] <- ","

  fromJSON(c("[", ret, "]"))
}


#' address_parse_libpostal
#'
#' @param address_stream data.table of addresses
#'
#' @return data.table of addresses parsed, one per unique address in address_stream
#' @importFrom tidyr unite
#' @importFrom stringr str_detect str_match str_remove fixed
#' @importFrom dplyr any_of distinct
#' @importFrom checkmate assert_data_table assert_names
#' @describeIn address_parse handle parsing by libpostal
address_parse_libpostal <- function(address_stream) {
  address <- unit <- postcode <- road <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream), must.include = address_cols)

  # one row per unique address
  address_stream <- address_stream[, ..address_cols] %>%
    distinct() %>%
    setDT()

  # make address string for libpostal
  address_stream[, address := unite(.SD, "address", sep = ", ", na.rm = TRUE), .SDcols = address_cols]
  addresses <- tolower(address_stream[!is.na(address), address])

  # TODO: map english numbers to numerals
  parsed <- data.table(address = addresses, address_exec_libpostal(addresses))
  parsed <- parsed[address_stream$address, on = "address"]
  parsed[, I := .I]

  # add columns if they don't exist
  parsed <- rbind(parsed, data.table(address = character(0), unit = character(0), postcode = character(0), road = character(0)), fill = TRUE)
  address_stream <- rbind(address_stream, data.table(street2 = character(0)), fill = TRUE)

  streets_regex <- tolower("(ALLEY|ALLEE|ALY|ALLY|ANEX|ANX|ANNEX|ANNX|ARCADE|ARC|AVENUE|AV|AVE|AVEN|AVENU|AVN|AVNUE|BAYOU|BAYOO|BYU|BEACH|BCH|BEND|BND|BLUFF|BLF|BLUF|BLUFFS|BLFS|BOTTOM|BOT|BTM|BOTTM|BOULEVARD|BLVD|BOUL|BOULV|BRANCH|BR|BRNCH|BRIDGE|BRDGE|BRG|BROOK|BRK|BROOKS|BRKS|BURG|BG|BURGS|BGS|BYPASS|BYP|BYPA|BYPAS|BYPS|CAMP|CP|CMP|CANYON|CANYN|CYN|CNYN|CAPE|CPE|CAUSEWAY|CSWY|CAUSWA|CENTER|CEN|CTR|CENT|CENTR|CENTRE|CNTER|CNTR|CENTERS|CTRS|CIRCLE|CIR|CIRC|CIRCL|CRCL|CRCLE|CIRCLES|CIRS|CLIFF|CLF|CLIFFS|CLFS|CLUB|CLB|COMMON|CMN|COMMONS|CMNS|CORNER|COR|CORNERS|CORS|COURSE|CRSE|COURT|CT|COURTS|CTS|COVE|CV|COVES|CVS|CREEK|CRK|CRESCENT|CRES|CRSENT|CRSNT|CREST|CRST|CROSSING|XING|CRSSNG|CROSSROAD|XRD|CROSSROADS|XRDS|CURVE|CURV|DALE|DL|DAM|DM|DIVIDE|DIV|DV|DVD|DRIVE|DR|DRIV|DRV|DRIVES|DRS|ESTATE|EST|ESTATES|ESTS|EXPRESSWAY|EXP|EXPY|EXPR|EXPRESS|EXPW|EXTENSION|EXT|EXTN|EXTNSN|EXTENSIONS|EXTS|FALL|FALLS|FLS|FERRY|FRY|FRRY|FIELD|FLD|FIELDS|FLDS|FLAT|FLT|FLATS|FLTS|FORD|FRD|FORDS|FRDS|FOREST|FRST|FORESTS|FORGE|FORG|FRG|FORGES|FRGS|FORK|FRK|FORKS|FRKS|FORT|FT|FRT|FREEWAY|FWY|FREEWY|FRWAY|FRWY|GARDEN|GDN|GARDN|GRDEN|GRDN|GARDENS|GDNS|GRDNS|GATEWAY|GTWY|GATEWY|GATWAY|GTWAY|GLEN|GLN|GLENS|GLNS|GREEN|GRN|GREENS|GRNS|GROVE|GROV|GRV|GROVES|GRVS|HARBOR|HARB|HBR|HARBR|HRBOR|HARBORS|HBRS|HAVEN|HVN|HEIGHTS|HT|HTS|HIGHWAY|HWY|HIGHWY|HIWAY|HIWY|HWAY|HILL|HL|HILLS|HLS|HOLLOW|HLLW|HOLW|HOLLOWS|HOLWS|INLET|INLT|ISLAND|IS|ISLND|ISLANDS|ISS|ISLNDS|ISLE|ISLES|JUNCTION|JCT|JCTION|JCTN|JUNCTN|JUNCTON|JUNCTIONS|JCTNS|JCTS|KEY|KY|KEYS|KYS|KNOLL|KNL|KNOL|KNOLLS|KNLS|LAKE|LK|LAKES|LKS|LAND|LANDING|LNDG|LNDNG|LANE|LN|LIGHT|LGT|LIGHTS|LGTS|LOAF|LF|LOCK|LCK|LOCKS|LCKS|LODGE|LDG|LDGE|LODG|LOOP|LOOPS|MALL|MANOR|MNR|MANORS|MNRS|MEADOW|MDW|MEADOWS|MDWS|MEDOWS|MEWS|MILL|ML|MILLS|MLS|MISSION|MISSN|MSN|MSSN|MOTORWAY|MTWY|MOUNT|MNT|MT|MOUNTAIN|MNTAIN|MTN|MNTN|MOUNTIN|MTIN|MOUNTAINS|MNTNS|MTNS|NECK|NCK|ORCHARD|ORCH|ORCHRD|OVAL|OVL|OVERPASS|OPAS|PARK|PRK|PARKS|PARKWAY|PKWY|PARKWY|PKWAY|PKY|PARKWAYS|PKWYS|PASS|PASSAGE|PSGE|PATH|PATHS|PIKE|PIKES|PINE|PNE|PINES|PNES|PLACE|PL|PLAIN|PLN|PLAINS|PLNS|PLAZA|PLZ|PLZA|POINT|PT|POINTS|PTS|PORT|PRT|PORTS|PRTS|PRAIRIE|PR|PRR|RADIAL|RAD|RADL|RADIEL|RAMP|RANCH|RNCH|RANCHES|RNCHS|RAPID|RPD|RAPIDS|RPDS|REST|RST|RIDGE|RDG|RDGE|RIDGES|RDGS|RIVER|RIV|RVR|RIVR|ROAD|RD|ROADS|RDS|ROUTE|RTE|ROW|RUE|RUN|SHOAL|SHL|SHOALS|SHLS|SHORE|SHOAR|SHR|SHORES|SHOARS|SHRS|SKYWAY|SKWY|SPRING|SPG|SPNG|SPRNG|SPRINGS|SPGS|SPNGS|SPRNGS|SPUR|SPURS|SQUARE|SQ|SQR|SQRE|SQU|SQUARES|SQRS|SQS|STATION|STA|STATN|STN|STRAVENUE|STRA|STRAV|STRAVEN|STRAVN|STRVN|STRVNUE|STREAM|STRM|STREME|STREET|ST|STRT|STR|STREETS|STS|SUMMIT|SMT|SUMIT|SUMITT|TERRACE|TER|TERR|THROUGHWAY|TRWY|TRACE|TRCE|TRACES|TRACK|TRAK|TRACKS|TRK|TRKS|TRAFFICWAY|TRFY|TRAIL|TRL|TRAILS|TRLS|TRAILER|TRLR|TRLRS|TUNNEL|TUNEL|TUNL|TUNLS|TUNNELS|TUNNL|TURNPIKE|TRNPK|TPKE|TURNPK|UNDERPASS|UPAS|UNION|UN|UNIONS|UNS|VALLEY|VLY|VALLY|VLLY|VALLEYS|VLYS|VIADUCT|VDCT|VIA|VIADCT|VIEW|VW|VIEWS|VWS|VILLAGE|VILL|VLG|VILLAG|VILLG|VILLIAGE|VILLAGES|VLGS|VILLE|VL|VISTA|VIS|VIST|VST|VSTA|WALK|WALKS|WALL|WAY|WY|WAYS|WELL|WL|WELLS|WLS)")
  directions_regex <- tolower("(N|NE|NW|S|SE|SW|E|W|NORTE|NORESTE|NOROESTE|SUR|SURESTE|SUROESTE|ESTE|OESTE|NORTH|NORTHEAST|NORTHWEST|SOUTH|SOUTHEAST|SOUTHWEST|EAST|WEST)")
  unit_regex <- tolower("(Apartment(?!$)|APT(?!$)|Basement|BSMT|Building(?!$)|BLDG(?!$)|Department(?!$)|DEPT(?!$)|Floor|FL|Front|FRNT|Hanger(?!$)|HNGR(?!$)|Key(?!$)|KEY(?!$)|Lobby|LBBY|Lot(?!$)|LOT(?!$)|LOWR|Office|OFC|Penthouse|PH|Pier(?!$)|PIER(?!$)|Rear|REAR|Room(?!$)|RM(?!$)|Slip(?!$)|SLIP(?!$)|Space(?!$)|SPC(?!$)|Stop(?!$)|STOP(?!$)|Suite(?!$)|STE(?!$)|Trailer(?!$)|TRLR(?!$)|Unit(?!$)|UNIT(?!$)|UPPR|#(?!$))")
  unit_number_regex <- "(\\d[\\d\\w]*|\\w\\d+|ph.{1,3})"
  unit_regex2 <- paste0("^", unit_regex, "?\\s*", unit_number_regex, "$")
  street_unit_regex <- paste0("\\W+", streets_regex, "\\W+", "(", directions_regex, "\\W+", ")?", "((", unit_regex, "|", unit_number_regex, ")")

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
  parsed[
    is.na(unit) & postcode != address_stream$postal_code[I],
    unit := postcode
  ]
  # ... some units don't get detected in street2
  parsed[
    is.na(unit) & str_detect(address_stream$street2[I], unit_regex2),
    unit := address_stream$street2[I]
  ]
  # ... and if the road has the unit in it, put it in unit
  parsed[
    is.na(unit) & str_detect(road, paste0(street_unit_regex, ")")),
    unit := str_match(road, paste0(street_unit_regex, ".*$)"))[, 5]
  ]
  # ... finally cleanup unit
  parsed[, unit := trimws(unit)]
  parsed[unit == "", unit := NA]
  # And remove it from other fields if it's duplicated
  lapply(
    intersect(c("postcode", "road", "house"), colnames(parsed)),
    function(col) {
      parsed[!is.na(unit), (col) := str_remove(
        get(col),
        # Escape the unit so that it can work as a regex
        paste0("(^|\\W+)", str_replace_all(unit, "[^a-z0-9]", "\\$0"), "(\\W+|$)")
      )]
      parsed[!is.na(unit) & get(col) == "", (col) := NA_character_]
    }
  )

  # Now merge everything else together
  if (any(c("unit", "level", "entrance", "staircase") %in% colnames(parsed))) {
    parsed <- parsed %>% unite("unit", any_of(c("unit", "level", "entrance", "staircase")), sep = " ", na.rm = TRUE)
  }
  if (any(c("house", "category", "near") %in% colnames(parsed))) {
    parsed <- parsed %>% unite("house", any_of(c("house", "category", "near")), sep = " ", na.rm = TRUE)
  }
  if (any(c("suburb", "city_district", "city", "island") %in% colnames(parsed))) {
    parsed <- parsed %>% unite("city", any_of(c("suburb", "city_district", "city", "island")), sep = ", ", na.rm = TRUE)
  }
  if (any(c("state_district", "state") %in% colnames(parsed))) {
    parsed <- parsed %>% unite("state", any_of(c("state_district", "state")), sep = ", ", na.rm = TRUE)
  }
  if (any(c("country_region", "country", "world_region") %in% colnames(parsed))) {
    parsed <- parsed %>% unite("country", any_of(c("country_region", "country", "world_region")), sep = ", ", na.rm = TRUE)
  }

  # ok maybe we're finally done. Let's clean up
  parsed_cols <- intersect(colnames(parsed),c("house_number", "road", "unit", "house", "po_box", "city", "state", "country", "postcode"))

  lapply(
    colnames(parsed),
    function(col) {
      parsed[get(col) == "", (col) := NA_character_]
    }
  )

  address_stream <- cbind(address_stream[, ..address_cols], libpostal = parsed[, ..parsed_cols])
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
  address_cache(address_stream, "address_parse", address_parse_libpostal)
}

#' address_cache
#'
#' Handles caching of already-processed addresses so that they're only processed once
#'
#' @param address_stream data.table of addresses
#' @param cache_name name of cache file
#' @param .function function to be called for processing, is sent `address_stream[address_cols]` and additional parameters.
#' @param db_name path to sqlite database, defaults to `tessilake:::cache_path("address_stream.sqlite","deep","stream")`
#' @param ... additional options passed to `.function`
#'
#' @return data.table of addresses processed
#' @importFrom dplyr collect tbl
#' @importFrom utils head capture.output
address_cache <- function(address_stream, cache_name, .function,
                          db_name = tessilake:::cache_path("address_stream.sqlite", "deep", "stream"), ...) {
  assert_data_table(address_stream)

  if (!dir.exists(dirname(db_name))) {
    dir.create(dirname(db_name))
  }

  cache_db <- DBI::dbConnect(RSQLite::SQLite(), db_name)
  withr::defer(DBI::dbDisconnect(cache_db))

  address_cols <- intersect(address_cols, colnames(address_stream))

  address_stream_distinct <- address_stream[,..address_cols] %>% distinct

  if (!DBI::dbExistsTable(cache_db, cache_name)) {
    # Cache doesn't yet exist
    cache <- NULL
    cache_miss <- address_stream_distinct
    db_function <- DBI::dbWriteTable

  } else {
    cache <- DBI::dbReadTable(cache_db, cache_name) %>% setDT()
    cache_miss <- address_stream_distinct[!cache, ..address_cols, on = as.character(address_cols)]
    db_function <- DBI::dbAppendTable
  }

  if(nrow(cache_miss) > 0) {
    cache_miss <- .function(cache_miss, ...)

    if (DBI::dbExistsTable(cache_db, cache_name)) {

      cache_schema <- tbl(cache_db, cache_name) %>% head %>% collect %>% sapply(typeof)
      input_schema <- sapply(cache_miss,typeof)
      matching_names <- intersect(names(cache_schema), names(input_schema))

      if(any(cache_schema[matching_names] != input_schema[matching_names]))
        rlang::abort(c("Identical column names used for different column types, can't cache table.",
                      "cache schema:",
                      capture.output(print(cache_schema[matching_names])),
                      "input_schema:",
                      capture.output(print(input_schema[matching_names]))))

      new_columns <- input_schema[setdiff(names(input_schema), names(cache_schema))]
      purrr::imap(new_columns,~DBI::dbExecute(cache_db, paste0("ALTER TABLE ",cache_name," ADD COLUMN [",.y,"] [",.x,"]")))
    }

    db_function(cache_db, cache_name, cache_miss)

    cache <- rbind(cache, cache_miss, fill = TRUE)
  }

  address_stream <- cache[address_stream[, ..address_cols], on = as.character(address_cols)]

  return(address_stream)
}
