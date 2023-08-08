#' contribution_stream
#'
#' Creates timestamped dataset of all contributions and caches it.
#' Adds creditee information, categorizes contributions based on campaign and NRR status, and associates with
#' memberships, using [contribution_membership_match].
#'
#' Output Features are:
#' *  group_customer_no (donor or creditee)
#' *  timestamp : date of contribution
#' *  event_type : Contribution
#' *  event_subtype : Membership|Gala|Other
#' *  event_subtype2 : New|Renew|Reinstate|Upgrade
#' *  cont_amt
#' *  campaign_no (foreign key for campaign)
#' *  ref_no (primary key of contributions)
#' *  cust_memb_no (foreign key for memberships)
#' *  contribution_timestamp_min
#' *  contribution_timestamp_max
#' *  contribution_timestamp_last
#' *  contribution_SUBTYPE_count
#' *  contribution_amt
#' *  contribution_max
#'
#' @param depth string, e.g. "deep" or "shallow", where to save the cache, passed to [tessilake::write_cache]
#' @param ... additional arguments passed on to [tessilake::read_tessi] and friends.
#' @param type string, e.g. "tessi" or "stream", where to save the cache, passed to [tessilake::write_cache]
#' @param event_subtypes list of formulas to be used for coding, as for [dplyr::case_when]. Can refer to any columns in the `contributions` table. See **Note** below
#' @note
#' * __event_subtypes__ This value for event_subtypes:
#'   ```
#'   list(
#'      !is.na(cust_memb_no) ~ "Membership",
#'      grepl("BENE",campaign_desc) ~ "Gala",
#'      TRUE ~ campaign_category_desc
#'      )
#'   ```
#'   Assigns
#'   * everything with a `cust_memb_no` to "Membership"
#'   * everything else with "BENE" in `campaign_desc` as "Gala", and
#'   * everything else to the value of `campaign_category_desc`
#' @export
#' @importFrom lubridate floor_date ddays
#' @importFrom dplyr case_when
#' @importFrom tessilake write_cache
contribution_stream <- function(depth = "deep", type = "stream", event_subtypes = list(TRUE ~ campaign_category_desc), ...) {

  event_subtypes <- substitute(event_subtypes)
  data <- contribution_stream_load()
  contributions <- contribution_membership_match(data$contributions, data$memberships)
  setkey(contributions, group_customer_no, cont_dt)

  contribution_stream = contributions[cont_amt > 0] %>%
    .[,`:=`(
      contribution_amt = cumsum(cont_amt),
      contribution_max = cummax(cont_amt),
      contribution_timestamp_min = min(cont_dt),
      contribution_timestamp_max = cont_dt), by="group_customer_no"] %>%
    merge(.[,.(cont_dt = unique(cont_dt),
               contribution_timestamp_last = lag(unique(cont_dt))), by = "group_customer_no"],
          by = c("group_customer_no","cont_dt")) %>%
    .[,.(group_customer_no,
         timestamp = cont_dt,
         event_type = "Contribution",
         event_subtype = case_when(!!!eval(event_subtypes)),
         cont_amt,
         campaign_no,ref_no,cust_memb_no,
         contribution_amt,contribution_max,
         contribution_timestamp_min,contribution_timestamp_max,contribution_timestamp_last)]

  # set contribution_SUBTYPE_count
  for(subtype in unique(contribution_stream$event_subtype))
    contribution_stream[,(paste0("contribution_",subtype,"_count")) := cumsum(event_subtype == subtype), by = "group_customer_no"]

  # set event_subtype2
  contribution_stream[,event_subtype2 := case_when(
    1:.N == 1 ~ "New",
    !is.na(cust_memb_no) & duplicated(cust_memb_no) | is.na(cust_memb_no) & duplicated(campaign_no) ~ "Upgrade",
    timestamp - lag(timestamp) <= ddays(365) ~ "Renew",
    TRUE ~ "Reinstate"), by=c("group_customer_no","event_subtype")]


  write_cache(contribution_stream, "contribution_stream", depth = depth, type = type)

  contribution_stream
}

#' @describeIn contribution_stream load data for contribution_stream
#' @usage NULL
#' @importFrom dplyr collect filter left_join rename
contribution_stream_load <- function(...) {
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

  memberships <- read_tessi("memberships", ...) %>% filter(!current_status_desc %in% c("Cancelled","Deactivated")) %>%
    left_join(read_tessi("campaigns", select = c("campaign_no", "fyear", "category_desc"), ...),
              by = "campaign_no", suffix = c("",".campaign")) %>%
    left_join(read_sql_table("T_MEMB_LEVEL"),
              by = c("memb_level_no","memb_org_no"), suffix = c("",".level")) %>%
    rename(campaign_category_desc = category_desc) %>%
    collect %>% setDT

  return(list(contributions = contributions,
              memberships = memberships))
}



#' contribution_membership_match
#'
#' Match memberships to contributions based on matching `create_dt`, `init_dt`/`cont_dt`, and alignment between
#' `memb_amt + AVC_amt`, `recog_amt`, `start_amt` and the `cont_amt` of the contribution. Expected to fix 90% of
#' unmatched contributions.
#'
#' @param contributions data.table of contributions, must include columns `group_customer_no`, `ref_no`, `cust_memb_no`, `campaign_category_desc`, `cont_amt`, `create_dt`, `cont_dt`
#' @param memberships data.table of memberships, must include columns `group_customer_no`, `cust_memb_no`, `campaign_category_desc`, `memb_amt`, `AVC_amt`, `recog_amt`, `start_amt`, `create_dt`, `init_dt`
#'
#' @return data.table of contributions with `cust_memb_no` updated to match the appropriate membership in `memberships`
#' @importFrom data.table copy
#' @importFrom checkmate assert_named
contribution_membership_match <- function(contributions,memberships) {
  assert_names(colnames(contributions), must.include = c("group_customer_no", "ref_no", "cust_memb_no", "campaign_category_desc", "cont_amt", "create_dt", "cont_dt"))
  assert_names(colnames(memberships), must.include = c("group_customer_no", "cust_memb_no", "campaign_category_desc", "memb_amt", "AVC_amt", "recog_amt", "start_amt", "create_dt", "init_dt"))

  contributions <- copy(contributions)
  memberships <- copy(memberships)

  # iterate until we've matched all contributions
  n_old <- -1
  while(n_old < contributions[!is.na(cust_memb_no),.N]) {
    n_old <- contributions[!is.na(cust_memb_no),.N]
    set_contribution_membership_match(contributions, memberships)
  }

  contributions
}



#' @describeIn contribution_membership_match updates `contributions$cust_memb_no` for up to one contribution per membership, intended to be called iteratively from within [contribution_membership_match]
set_contribution_membership_match <- function(contributions, memberships) {
  # calculate membership amount based on contributions already associated with it
  memberships[contributions[!is.na(cust_memb_no),.(cont_amt=sum(cont_amt)),by="cust_memb_no"],
              cont_amt := i.cont_amt,
              on = "cust_memb_no"]
  memberships[is.na(cont_amt), cont_amt := 0]

  memberships[,value_amt := pmax(memb_amt + AVC_amt,recog_amt,start_amt)]
  unmatched_contributions <- contributions[is.na(cust_memb_no) & cont_amt>0]

  match_data <- memberships[unmatched_contributions,  .(group_customer_no,
                                                        cust_memb_no,ref_no = i.ref_no,
                                                        value_amt, cont_amt, i.cont_amt,
                                                        campaign_category_desc,i.campaign_category_desc,
                                                        create_dt,i.create_dt,
                                                        init_dt,cont_dt),
                                      on="group_customer_no",allow.cartesian = TRUE]

  match_data[  value_amt >= cont_amt + i.cont_amt
             ,`:=`(score =  i.cont_amt/(value_amt - cont_amt) +
                            (campaign_category_desc == i.campaign_category_desc) +
                            -abs(create_dt-i.create_dt)/ddays(365) +
                            -abs(init_dt-cont_dt)/lubridate::ddays(365))]

  setkey(match_data,score)
  contributions[match_data[score>0,.SD[.N],by="cust_memb_no"],cust_memb_no:=i.cust_memb_no,on = "ref_no"]

}

