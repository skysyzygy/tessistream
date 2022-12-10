# Census Geocoding --------------------------------------------------------

#' address_geocode_census
#'
#' Geocodes addresses using the US census geocoding API. Tries each address up to three times, using `libpostal` parsing, street1, and street2.
#'
#' @param address_stream data.table of addresses, must include `address_cols` and `timestamp`
#'
#' @return data.table of addresses, one row per unique parseable address, with geocode information appended
#'
#' @importFrom censusxy cxy_benchmarks cxy_vintages cxy_geocode
#' @importFrom stringr str_extract
#' @importFrom lubridate year today
#' @importFrom dplyr coalesce
#' @importFrom tessilake setleftjoin
address_geocode_census <- function(address_stream) {
  . <- isDefault <- id <- vintageName <- timestamp <- street1 <- street2 <- city <- state <- postal_code <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream),must.include = c("timestamp",address_cols))

  # Prepare for census geocoding
  # benchmark is the time of the data snapshot
  benchmark = cxy_benchmarks() %>% setDT %>% .[isDefault == TRUE,id]
  # Vintage is the year of census tracts to look at
  vintages = cxy_vintages(benchmark) %>% setDT %>%
    .[,year:=str_extract(vintageName,"\\d+") %>% as.integer] %>%
    .[!is.na(year)] %>%
    # associate one vintage to each year
    .[data.table(year=seq(1970,year(today()))),on="year",roll=-Inf]

  address_stream[,year:=year(timestamp)]
  setleftjoin(address_stream,vintages,by="year")

  census = rbind(address_stream[,.(street=street1,city,state,postal_code,vintageName,type="street1")],
                 address_stream[,.(street=street2,city,state,postal_code,vintageName,type="street2")])

  # load address_parse data from database
  if(file.exists(tessilake:::cache_path("address_stream.sqlite","deep","stream"))) {
    db <- DBI::dbConnect(RSQLite::SQLite(), tessilake:::cache_path("address_stream.sqlite","deep","stream"))
    address_parse <- DBI::dbReadTable(db,"address_parse")
    setleftjoin(address_stream,address_parse,by=as.character(address_cols))

    census = rbind(census,
                   address_stream[,.(street=paste(libpostal.house_number,libpostal.road),
                                     city=libpostal.city,state=libpostal.state,
                                     postal_code=libpostal.postcode,vintageName,type="libpostal")])
    DBI::dbDisconnect(db)
  }

  # build queue of addresses to test
  address_stream <- cbind(address_stream[,..address_cols],census=census) %>%
  # only look at addresses that we have a chance of matching with the census matcher.
    .[ !is.na(census.state) & !is.na(census.postal_code) & !is.na(census.city) & !is.na(census.vintageName) &
      grepl("^\\d{5,}$",census.postal_code) & grepl("[1-9]",census.postal_code) &
      grepl("^[A-z]",census.city) & grepl("^[A-z]",census.state) ]

  geocode <- address_stream[census.type=="libpostal"] %>%
                              address_exec_census() %>% .[ cxy_quality == "Exact" ]

  geocode <- address_stream[census.type=="street1"][!geocode,on=c(as.character(address_cols),"census.vintageName")] %>%
    address_exec_census() %>% .[ cxy_quality == "Exact" ] %>% rbind(geocode)

  geocode <- address_stream[census.type=="street2"][!geocode,on=c(as.character(address_cols),"census.vintageName")] %>%
    address_exec_census() %>% .[ cxy_quality == "Exact" ] %>% rbind(geocode)

  geocode

}


#' address_exec_census
#'
#' @param address_stream data.table of addressses, containing at least `census.street`, `census.city`, `census.state`, `census.postal_code`, `census.vintageName`
#'
#' @return data.table with census geocoding data appended as additional columns
#' @importFrom censusxy cxy_geocode cxy_benchmarks
#' @importFrom data.table rbindlist
#'
address_exec_census <- function(address_stream) {
  . <- isDefault <- id <- census.vintageName <- NULL

  assert_data_table(address_stream)
  assert_names(colnames(address_stream),must.include = c("census.street","census.city","census.state","census.postal_code","census.vintageName"))

  if(nrow(address_stream) == 0)
    return(cbind(address_stream,cxy_quality=character(0)))

  benchmark = cxy_benchmarks() %>% setDT %>% .[isDefault == TRUE,id]

  address_stream[,cxy_geocode(.SD,street = "census.street",city = "census.city",state = "census.state",zip = "census.postal_code",vintage = census.vintageName[[1]],
              return = "geographies",benchmark = benchmark,output = "full",class = "dataframe"),
  by="census.vintageName"]

}

  if(FALSE) {

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

}
