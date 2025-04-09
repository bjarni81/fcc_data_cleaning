require(tidyverse)
require(readr)
require(tictoc)
require(crayon)
require(tidycensus)
#==there was a 26.6% reduction in the number of census blocks from 2010 to 2020
#https://www.census.gov/geographies/reference-files/time-series/geo/tallies.html#block_by_state
#this likely explains why there are so many more census blocks in the 2020 and 2021 files
#--
#as of running these programs I know of no other way than to download the whole
  #year and read the whole .csv at once
#download .csv files:
#https://opendata.fcc.gov/api/views/4kuc-phrr/rows.csv?accessType=DOWNLOAD&sorting=true
#https://opendata.fcc.gov/api/views/hicn-aujz/rows.csv?accessType=DOWNLOAD&sorting=true
#https://opendata.fcc.gov/api/views/jdr4-3q4p/rows.csv?accessType=DOWNLOAD&sorting=true
#---------
#connect to sql
oabi_con <- DBI::dbConnect(odbc::odbc(),
                           Driver = "SQL Server",
                           Server = "vhacdwsql13.vha.med.va.gov",
                           Database = "OABI_MyVAAccess",
                           Trusted_Connection = "true")
#
`%ni%` = purrr::negate(`%in%`)
#location of downloaded files
file_locs = list.files("\\\\r02iowhsm201.v23.med.va.gov\\Research\\QI\\Kaboli_Access_Evaluation\\Bjarni\\fcc_data_download_and_cleaning",
                       full.names = TRUE,
                       pattern = ".csv")
#
col_names = c("lrn", "prov_id", "frn", "prov_name", "dba_name", "hco_name", "hco_num", "hco_fin", "state",
              "census_block", "tech_code", "consumer", "max_ds", "max_us", "business")
#restrictions
filter_fxn = function(x, pos){filter(x, .data[["consumer"]] == 1
                                     & .data[["tech_code"]] != "60")}#excluding satellite
#df for iterating over
data(fips_codes, package = "tidycensus")
state_abb <- fips_codes %>%
  filter(state_code < 60) %>%
  select(state, state_code, state_name) %>%
  distinct
#==
#function for reading and cleaning
summ_fxn = function(table_lab, table_month, table_year){
  csv_file = list.files("\\\\r02iowhsm201.v23.med.va.gov\\Research\\QI\\Kaboli_Access_Evaluation\\Bjarni\\fcc_data_download_and_cleaning",
                        full.names = TRUE,
                        pattern = paste0({{table_month}}, "\\S*(", {{table_year}}, ")"))
  tictoc::tic("reading .csv")
  full_csv = read_csv_chunked(csv_file,
                              DataFrameCallback$new(filter_fxn),
                              chunk_size = 200000,
                              col_types = "ddccccdccdddddd",
                              col_names = col_names)
  tictoc::toc()
  for (i in 1:nrow(state_abb)){#
    cat(magenta("starting", state_abb[i,3]))
    summarised_table = full_csv  %>%
      filter(state == state_abb[i,1]) %>%
      rename(state_usps = state,
             block_geoid = census_block) %>%
      mutate(ds_ge_25 = if_else(max_ds >= 25, 1, 0),
             ds_ge_50 = if_else(max_ds >= 50, 1, 0),
             ds_ge_100 = if_else(max_ds >= 100, 1, 0),
             us_ge_3 = if_else(max_us >= 3, 1, 0),
             us_ge_5 = if_else(max_us >= 5, 1, 0),
             us_ge_10 = if_else(max_us >= 10, 1, 0),
             us_ge_25 = if_else(max_us >= 25, 1, 0),
             us_ge_100 = if_else(max_us >= 100, 1, 0),
             meets_25_3 = if_else(max_ds >= 25 
                                  & max_us >= 3, 1, 0),
             meets_25_5 = if_else(max_ds >= 25 
                                  & max_us >= 5, 1, 0),
             meets_100_20 = if_else(max_ds >= 100 
                                    & max_us >= 20, 1, 0),
             meets_100_100 = if_else(max_ds >= 100 
                                     & max_us >= 20, 1, 0)) %>%
      group_by(block_geoid, state_usps) %>%
      summarise(n_25_3 = sum(meets_25_3),
                n_25_5 = sum(meets_25_5),
                n_100_20 = sum(meets_100_20),
                n_100_100 = sum(meets_100_100),
                n_prov = n()) %>%
      left_join(., state_abb %>%
                  select(state_usps = state, state_fips = state_code)) %>%
      rename_with(., ~ paste0(.x, "_", {{table_lab}}),
                  contains(c("n_25", "n_100", "prov"))) %>%
      mutate(block_geoid = as.character(block_geoid))
    #----
    table_id <- DBI::Id(schema = "crh_eval", table = paste0("bband_summary_", 
                                                            {{table_lab}}))
    tic(magenta(paste0("pushing ", state_abb[i,3], " to sql")))
    #sequence for iterating over
    tab_seq <- c(seq(0, nrow(summarised_table), 10000), nrow(summarised_table))
    #
    for(j in 1:length(tab_seq)){
      k = j + 1
      if (tab_seq[j] < max(tab_seq)){
        DBI::dbWriteTable(conn = oabi_con,
                          name = table_id,
                          value = summarised_table[(tab_seq[j] + 1):(tab_seq[k]),],
                          append = TRUE)
      }
      else{}
    }
    toc()
  }
}
tictoc::tic("summarising")
summ_fxn("2021_12",table_month = "dec", 2021)
#rm(current_file)
tictoc::toc()
#
tictoc::tic("summarising")
summ_fxn("2021_06",table_month = "Jun", 2021)
#rm(current_file)
tictoc::toc()
#
tictoc::tic("summarising")
summ_fxn("2020_12",table_month = "Dec", 2020)
#rm(current_file)
tictoc::toc()
#
tictoc::tic("summarising")
summ_fxn("2020_06",table_month = "Jun", 2020)
#rm(current_file)
tictoc::toc()