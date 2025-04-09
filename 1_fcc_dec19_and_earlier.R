require(tidyverse)
require(tictoc)
require(crayon)
require(tidycensus)
#connect to sql
oabi_con <- DBI::dbConnect(odbc::odbc(),
                           Driver = "SQL Server",
                           Server = "vhacdwsql13.vha.med.va.gov",
                           Database = "OABI_MyVAAccess",
                           Trusted_Connection = "true")
#helper function
`%ni%` = purrr::negate(`%in%`)
#function for assembling urls
url_fxn = function(yearMonth, st){
  if(yearMonth == "Jun15"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Jun15/Version%205/",
                                  state_abb[st,1],
                                  "-Fixed-Jun2015.zip")}
  else if(yearMonth == "Dec15"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Dec15/Version%204/",
                                       state_abb[st,1],
                                       "-Fixed-Dec2015.zip")}
  else if(yearMonth == "Jun16"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Jun16/Version%204/",
                                       state_abb[st,1],
                                       "-Fixed-Jun2016.zip")}
  else if(yearMonth == "Dec16"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Dec16/Version%202/",
                                       state_abb[st,1],
                                       "-Fixed-Dec2016.zip")}
  else if(yearMonth == "Jun17"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Jun17/Version%203/",
                                       state_abb[st,1],
                                       "-Fixed-Jun2017.zip")}
  else if(yearMonth == "Dec17"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Dec17/Version%203/",
                                       state_abb[st,1],
                                       "-Fixed-Dec2017.zip")}
  else if(yearMonth == "Jun18"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Jun18/Version%201/",
                                       state_abb[st,1],
                                       "-Fixed-Jun2018.zip")}
  else if(yearMonth == "Dec18"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Dec18/Version%203/",
                                       state_abb[st,1],
                                       "-Fixed-Dec2018.zip")}
  else if(yearMonth == "Jun19"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Jun19/Version%202/",
                                       state_abb[st,1],
                                       "-Fixed-Jun2019.zip")}
  else if(yearMonth == "Dec19"){paste0("https://www.fcc.gov/form477/BroadbandData/Fixed/Dec19/Version%201/",
                                       state_abb[st,1],
                                       "-Fixed-Dec2019.zip")}
}
#----
#getting fips codes from the tidycensus package
data(fips_codes, package = "tidycensus")
state_abb <- fips_codes %>%
  filter(state_code < 60) %>%#restriction
  select(state, state_code, state_name) %>%
  distinct
#vector of column names
col_names = c("lrn", "prov_id", "frn", "prov_name", "dba_name", "hco_name", "hco_num", "hco_fin", "state",
              "census_block", "tech_code", "consumer", "max_ds", "max_us", "business")
#
#function for downloading and cleaning data from urls
clean_fxn = function(yearMonth, date_lab){
  for (st in 1:nrow(state_abb)){
    cat(bgWhite$black("starting loop", st, "\n"))
    tic(magenta(paste0("loop #", st, " (i.e., ", state_abb[st,1], ")")))
    #downloading the file
    download.file(url_fxn({{yearMonth}}, st),
                  destfile = "U:\\Users\\VHAIOWHaralB\\Desktop/temp_folder/out.zip",
                  method = "wininet")
    #unzipping and reading into R
    output_table = readr::read_csv(unzip(zipfile = "U:\\Users\\VHAIOWHaralB\\Desktop/temp_folder/out.zip",
                                         exdir = "U:\\Users\\VHAIOWHaralB\\Desktop/temp_folder"),
                                   show_col_types = FALSE,
                                   col_names = col_names) %>%
      filter(consumer == 1#restriction
             & tech_code != 60) %>%#restriction
      rename(state_usps = state,
             block_geoid = census_block) %>%
      mutate(max_ds = as.numeric(max_ds),
             max_us = as.numeric(max_us),
             ds_ge_25 = if_else(max_ds >= 25, 1, 0),
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
      rename_with(., ~ paste0(.x, "_", {{date_lab}}),
                  contains(c("n_25", "n_100", "prov"))) %>%
      mutate(block_geoid = as.character(block_geoid))
    #------
    #deleting .zip and .csv files
    csv_path = list.files("U:\\Users\\VHAIOWHaralB\\Desktop/temp_folder/",
                          pattern = ".csv",
                          full.names = TRUE)
    zip_path = list.files("U:\\Users\\VHAIOWHaralB\\Desktop/temp_folder/",
                          pattern = ".zip",
                          full.names = TRUE)
    file.remove(csv_path[1])
    file.remove(zip_path[1])
    toc()
    gc()
    # outputting table to sql
    table_id <- DBI::Id(schema = "crh_eval", table = paste0("bband_summary_", {{date_lab}}))
    #sequence for iterating over
    tab_seq <- c(seq(0, nrow(output_table), 10000), nrow(output_table))
    #
    for(j in 1:length(tab_seq)){
      k = j + 1
      if (tab_seq[j] < max(tab_seq)){
        DBI::dbWriteTable(conn = oabi_con,
                          name = table_id,
                          value = output_table[(tab_seq[j] + 1):(tab_seq[k]),],
                          append = TRUE)
      }
      else{}
    }
    rm(output_table)
    gc()
  }
  #compress
  DBI::dbExecute(oabi_con,
                 str_remove_all(
                   paste0(
                     "ALTER TABLE [crh_eval].[bband_summary_", 
                     {{date_lab}}, 
                     "] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)"), 
                   c("\n|\t")))
}
tic(cyan("the whole run: "))
clean_fxn("Dec19", "2019_12")
toc()


