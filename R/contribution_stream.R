#' contribution_stream
#'
#' Creates timestamped dataset of all contributions and caches it.
#' Adds creditee information, categorizes memberships based on campaign and NRR status, and associates with
#' memberships.
#'
#' Output Features are:
#' *  group_customer_no (donor or creditee)
#' *  timestamp : date of contribution
#' *  event_type : Contribution
#' *  event_subtype : Major Gifts|Patron|Member|Other
#' *  event_subtype2 : New|Renew|Reinstate|Upgrade
#' *  cont_amt
#' *  campaign_no (foreign key for campaign)
#' *  ref_no (primary key of contributions)
#' *  cust_memb_no (foreign key for memberships)
#' *  contribution_timestamp_min
#' *  contribution_timestamp_max
#' *  contribution_SUBTYPE_count
#' *  contribution_amt
#' *  contribution_max
#'
#' @param depth string, e.g. "deep" or "shallow", where to save the cache, passed to [tessilake::write_cache]
#' @param ... additional arguments passed on to [tessilake::read_tessi]
#' @param type string, e.g. "tessi" or "stream", where to save the cache, passed to [tessilake::write_cache]
#' @export
#' @importFrom lubridate floor_date ddays
#' @importFrom dplyr collect filter left_join
contribution_stream <- function(depth = "deep", type = "stream", ...) {
  # Load data from Tessi
  contributions = read_tessi("contributions", ...) %>%
  # Needed because the BI table filterse out some contribution references
    left_join(read_sql_table("TX_CONT_MEMB", ...),
              by = c("ref_no" = "cont_ref_no"), all.x = T, suffix = c(".old","")) %>%
    left_join(read_tessi("creditees", select = c("ref_no", "creditee_no"), ...),
              by = "ref_no", all.x = T, suffix = c("",".creditee")) %>%
    collect %>% setDT
  # Add creditee info
  contributions[!is.na(group_creditee_no),`:=`(group_customer_no=group_creditee_no, customer_no=creditee_no)]

  campaigns <- read_tessi("campaigns", select = c("campaign_no", "fyear", "category_desc"), ...) %>% collect %>% setDT
  levels <- read_sql_table("T_MEMB_LEVEL")
  memberships <- read_tessi("memberships", ...) %>% filter(!current_status_desc %in% c("Cancelled","Deactivated")) %>% collect %>% setDT
  memberships <- merge(memberships,campaigns,by="campaign_no",suffixes = c("",".campaign")) %>%
    merge(levels, by = c("memb_level_no","memb_org_no"), suffixes = c("",".level")) %>%
    setnames("category_desc","campaign_category_desc")

  # TODO: should we be working with transaction data? The timing might be more accurate?

  contribution_stream = contributions %>%
    filter(cont_amt>0) %>%
    left_join(campaigns %>% select(campaign_no,fyear),by="campaign_no",copy=TRUE) %>%
    left_join(select(membershipStream,cust_memb_no,event_subtype=event_subtype2),by="cust_memb_no",copy=TRUE) %>%
    mutate_if(is.factor,as.character) %>% setDT %>%
    setkey(group_customer_no,cont_dt) %>%
    .[,`:=`(
      event_subtype=case_when(
        !is.na(event_subtype)~event_subtype,
        grepl("Patron|Membership|Major|Friends of BAM|FOB|Cinema Club",campaign_desc,ignore.case=TRUE) & cont_amt<1500 ~ "Member",
        grepl("Patron|Membership|Major|Friends of BAM|FOB|Cinema Club",campaign_desc,ignore.case=TRUE) & cont_amt<10000 ~ "Patron",
        grepl("Patron|Membership|Major|Friends of BAM|FOB|Cinema Club",campaign_desc,ignore.case=TRUE) ~ "Major Gifts",
        grepl("BENE",campaign_desc) ~ "Gala",
        TRUE ~ "Other"),
      contributionTimestampMax=floor_date(cont_dt,"day"),
      fyear=coalesce(fyear,0))] %>%
    .[,`:=`(
      contributionCount = 1:.N,
      contributionMemberCount = cumsum(event_subtype=="Member"),
      contributionPatronCount = cumsum(event_subtype=="Patron"),
      contributionMGCount = cumsum(event_subtype=="Major Gifts"),
      contributionAmt = cumsum(cont_amt),
      contributionMax = cummax(cont_amt),
      contributionAdjAmt = cumsum(cont_amt_adj),
      contributionAdjMax = cummax(cont_amt_adj),
      contributionTimestampMin=min(cont_dt)),by="group_customer_no"] %>%
    .[,`:=`(row = 1:.N,
            fyear.last = lag(fyear),
            cont_dt.last = lag(cont_dt)),by=c("group_customer_no","event_subtype")] %>%
    merge(unique(.[,.(group_customer_no,contributionTimestampMax)])[,contributionTimestampLast:=lag(contributionTimestampMax),
                                                                    by="group_customer_no"],
          by=c("group_customer_no","contributionTimestampMax"),all.x=T) %>%
    .[,.(group_customer_no,timestamp=cont_dt,ref_no,cust_memb_no,event_type="Contribution",event_subtype,
         cont_amt,cont_amt_adj,campaign_no,fyear,
         contributionCount,contributionMemberCount,contributionPatronCount,contributionMGCount,
         contributionAmt,contributionMax,contributionAdjAmt,contributionAdjMax,
         contributionTimestampMin,contributionTimestampMax,contributionTimestampLast,row,fyear.last,cont_dt.last)]


  # Correct the stream to reflect membership renewals
  contribution_stream[membershipStream,event_subtype2:=i.event_subtype3,on="cust_memb_no"] %>%
    # And then fill in the rest
    .[is.na(event_subtype2),`:=`(event_subtype2 = case_when(
      fyear.last==fyear ~ "Upgrade",
      difftime(timestamp,cont_dt.last,"days")<dyears(1) ~ "Renew",
      TRUE ~ "Reinstate"
    )),by=c("group_customer_no")] %>%
    setkey(group_customer_no,timestamp) %>%
    # Label upgrade contributions
    .[!is.na(cust_memb_no),`:=`(event_subtype2 = if_else(lag(cust_memb_no)==cust_memb_no,
                                                         "Upgrade",as.character(event_subtype2)))]

  contribution_stream[,c("row","fyear.last","cont_dt.last"):=NULL]


  # TEST: all contributions made it through
  testit::assert(
    full_join(
      contribution_stream %>% group_by(year=year(timestamp)) %>% summarise(cont_amt=sum(cont_amt)),
      c %>% group_by(year=year(cont_dt)) %>% summarise(cont_amt=sum(cont_amt)),
      by="year") %>%
      filter(cont_amt.x!=cont_amt.y | is.na(cont_amt.x) | is.na(cont_amt.y)) %>% nrow == 0
  )

  # TEST: NAs are less than .1% of each column (except for columns where we expect it...)
  testit::assert(all(sapply(contribution_stream[,-c("cust_memb_no","contributionTimestampLast")],function(.){sum(is.na(.))<.001*length(.)})))

  # TEST: one and only one new membership per customer in 99.9% of cases
  testit::assert(contribution_stream[event_subtype %in% c("Member","Patron","Major Gifts"),
                                    sum(event_subtype2=="New"),by="group_customer_no"][V1>1,.N] <
                   .002*contribution_stream[event_subtype %in% c("Member","Patron","Major Gifts"),.N,by="group_customer_no"][,.N])

  # TEST: renewal rate is in a sensible range
  testit::assert(
    between(contribution_stream[event_subtype2 %in% c("Renew","Reinstate","Upgrade"),.N]/
              contribution_stream[,.N],
            .7,.9))

  # TEST: no data leak in contributionTimestampLast
  testit::assert(contribution_stream[contributionTimestampLast==contributionTimestampMax | contributionTimestampLast==timestamp,.N]==0)

  cache_write(contribution_stream,"deep","stream")

  contribution_stream
}

#' @describeIn contribution_membership_match updates `contributions$cust_memb_no`, intended to be called from within [contribution_membership_match]
set_contribution_membership_match <- function(contributions, memberships) {
  # recalculate membership amount based on contributions already associated with it
  memberships[contributions[!is.na(cust_memb_no),.(cont_amt=sum(cont_amt)),by="cust_memb_no"],
              cont_amt := i.cont_amt,
              on = "cust_memb_no"]
  memberships[is.na(cont_amt), cont_amt := 0]

  unmatched_contributions <- contributions[is.na(cust_memb_no) & cont_amt>0]

  match_data <- memberships[unmatched_contributions,  .(group_customer_no,
                                                        cust_memb_no,ref_no = i.ref_no,
                                                        start_amt,
                                                        memb_amt,AVC_amt,cont_amt,recog_amt,i.cont_amt,
                                                        campaign_category_desc,i.campaign_category_desc,
                                                        create_dt,i.create_dt,
                                                        init_dt,cont_dt),
                                      on="group_customer_no",allow.cartesian = TRUE]

  match_data[  (memb_amt + AVC_amt >= cont_amt + i.cont_amt) |
               (recog_amt >= cont_amt + i.cont_amt) |
               (start_amt >= cont_amt + i.cont_amt)
             ,`:=`(score =  i.cont_amt/(pmax(memb_amt + AVC_amt,recog_amt,start_amt) - cont_amt) +
                            (campaign_category_desc == i.campaign_category_desc) +
                            -abs(create_dt-i.create_dt)/ddays(365) +
                            -abs(init_dt-cont_dt)/lubridate::ddays(365))]

  setkey(match_data,score)
  contributions[match_data[score>0,.SD[.N],by="cust_memb_no"],cust_memb_no:=i.cust_memb_no,on = "ref_no"]

}

#' contribution_membership_match
#'
#' Match memberships to contributions based on matching `create_dt`, `init_dt`/`cont_dt`, and alignment between
#' `memb_amt + AVC_amt, recog_amt, start_amt` and the `cont_amt` of the contribution. Expected to fix 90% of
#' unmatched contributions.
#'
#' @param contributions data.table of contributions, must include columns `group_customer_no, campaign_category_desc, cont_amt, create_dt, cont_dt`
#' @param memberships data.table of memberships, must include columns `group_customer_no, campaign_category_desc, memb_amt, AVC_amt, recog_amt, start_amt, create_dt, init_dt`
#'
#' @return matched data.table of contributions
#' @importFrom data.table copy
#' @importFrom checkmate assert_named
contribution_membership_match <- function(contributions,memberships) {
  assert_names(colnames(contributions), must.include = c("group_customer_no", "campaign_category_desc", "cont_amt", "create_dt", "cont_dt"))
  assert_names(colnames(memberships), must.include = c("group_customer_no", "campaign_category_desc", "memb_amt", "AVC_amt", "recog_amt", "start_amt", "create_dt", "init_dt"))

  contributions <- copy(contributions)

  # iterate
  n_old <- -1
  while(n_old < contributions[!is.na(cust_memb_no),.N]) {
    n_old <- contributions[!is.na(cust_memb_no),.N]
    set_contribution_membership_match(contributions, memberships)
  }

  contributions
}

