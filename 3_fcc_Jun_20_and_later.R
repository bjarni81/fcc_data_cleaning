#program for getting june 2022 and later state-level FCC broadband data from the
#fcc broadband api, cleaning it, and pushing it to sql
library(tidyverse)
library(httr2)
library(purrr)
library(magrittr)
library(here)
library(tictoc)
library(crayon)
library(scales)
#connect to sql
oabi_con <- DBI::dbConnect(odbc::odbc(),
                           Driver = "SQL Server",
                           Server = "vhacdwsql13.vha.med.va.gov",
                           Database = "OABI_MyVAAccess",
                           Trusted_Connection = "true")
#for filtering
`%ni%` = negate(`%in%`)
#for read/write of csv/zip
base_path <- "U:\\Users\\VHAIOWHaralB\\Desktop"
#api hash_key
#instructions for username/api_key can be obtained from https://us-fcc.box.com/v/bdc-data-downloads-output
  #and https://us-fcc.box.com/v/bdc-public-data-api-spec
api_key <- #redacted, keep this in api_key.txt
#
user_name = #redacted, keep this in api_key.txt
#base url for api queries
api_path = "https://broadbandmap.fcc.gov/api/public/map"
#vector of dates to iterate over
available_dates <- request(paste0(api_path, "/listAsOfDates")) |> 
  req_headers("username" = user_name,
              "hash_value" = api_key) |>
  req_perform() |> 
  resp_body_json() %$% 
  plyr::ldply(data, data.frame) %>%
  filter(data_type == "availability") %>%
  mutate(as_of_date = ymd(str_sub(as_of_date, end = 10)),
         date_lab = format(as_of_date, format = "%Y_%m")) %>%
  select(as_of_date, date_lab)
#df of state names and codes for iterating over and printing
data(fips_codes, package = "tidycensus")
state_abb <- fips_codes %>%
  filter(state_code < 60) %>%
  select(state, state_code, state_name) %>%
  distinct
#for identifying census blocks with zero population: 
  #https://www.census.gov/geographies/reference-files/2020/geo/2020addcountlisting.html
#=====================
tic(bgBlack$white("the whole loop"))
for (date in 4){#change to 1:nrow(available_dates) to do all
  tic(green(paste0("date ", available_dates[date, 1])))
  for (st in 1:nrow(state_abb)){#change to 1:nrow(state_abb) to do all
    #======
    #df of file_ids for iterating over and identifying .csv names
    #this is where most of the filtering occurs
    file_ids = request(paste0(api_path, "/downloads/listAvailabilityData/",
                              available_dates[date, 1])) |> 
      req_headers("username" = user_name,
                  "hash_value" = api_key) |> 
      req_url_query(category = "State") |> 
      req_perform() |> 
      resp_body_json() %$%
      data.table::rbindlist(data) %>%
      filter(file_type == "csv"
             & subcategory != "Provider List"
             & technology_code %ni% c(60, 61)#excluding all satellite
             & state_fips == state_abb[st,2]) %>%
      select(file_id, file_name, technology_code_desc)
    #======
    tic(bgWhite$black(state_abb[st,3], st, "out of 51 (", percent(st / 51, accuracy = 0.1), ")"))
    for (i in 1:nrow(file_ids)){
      tic(cyan("get .zip, read.csv,", state_abb[st,3], available_dates[date, 1], file_ids[i,3], "loop", i))
      #api query returns a .zip of a .csv and writes it to the temporary folder
      request(paste0(api_path, "/downloads/downloadFile/availability/", 
                     file_ids[i,1])) |> 
        req_headers("username" = user_name,
                    "hash_value" = api_key) |> 
        req_perform() %>%
        resp_body_raw() %>%
        brio::write_file_raw(., paste0(base_path, "/temp_folder", "/out.zip"))
      #unzip 
      unzip(zipfile = paste0(base_path, "/temp_folder", "/out.zip"),
            exdir = paste0(base_path, "/temp_folder"))
      #read csv
      current_table <- readr::read_csv(paste0(base_path, "/temp_folder/", file_ids[i,2], ".csv"),
                                       show_col_types = FALSE) %>%
        filter(business_residential_code %in% c("R", "X")) %>% #residential only or both
        select(-c(location_id, business_residential_code, h3_res8_id)) %>%#filtering-out columns that 
        #give duplicate rows per census block
        distinct# unclear if this is the best move; probably slows things down some
      #delete .zip and .csv
      file.remove(paste0(base_path, "/temp_folder/out.zip"))
      file.remove(paste0(base_path, "/temp_folder/", file_ids[i,2], ".csv"))
      toc()
      #concatenating all state technologies
      if(i == 1){
        bband_summary = current_table
      }
      else {
        bband_summary = bband_summary %>% 
          bind_rows(., current_table)
      }
      #summarise to census block
      output_table = bband_summary %>%
        mutate(meets_25_3 = if_else(max_advertised_download_speed >= 25 
                                    & max_advertised_upload_speed >= 3, 1, 0),
               meets_25_5 = if_else(max_advertised_download_speed >= 25 
                                    & max_advertised_upload_speed >= 5, 1, 0),
               meets_100_20 = if_else(max_advertised_download_speed >= 100 
                                      & max_advertised_upload_speed >= 20, 1, 0),
               meets_100_100 = if_else(max_advertised_download_speed >= 100 
                                       & max_advertised_upload_speed >= 20, 1, 0)) %>%
        group_by(block_geoid, state_usps) %>%
        summarise(n_25_3 = sum(meets_25_3),
                  n_25_5 = sum(meets_25_5),
                  n_100_20 = sum(meets_100_20),
                  n_100_100 = sum(meets_100_100),
                  n_prov = n()) %>%
        rename_with(., ~ paste0(.x, "_", available_dates[date, 2]),
                    contains(c("n_25", "n_100", "prov"))) %>%
        mutate(block_geoid = as.character(block_geoid),
               state_fips = str_sub(block_geoid, end = 2))
    }
    toc()
    #-------------
    #for loop to write state to sql
    #id for table
    table_id <- DBI::Id(schema = "crh_eval", table = paste0("bband_summary_", 
                                                            available_dates[date, 2]))
    tic(magenta(paste0("pushing ", state_abb[st,3], " to sql")))
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
    toc()
    rm(current_table)
    rm(output_table)
    rm(bband_summary)
    gc()
  }
  toc()
  rm(current_table)
  rm(output_table)
  rm(bband_summary)
  gc()
  DBI::dbExecute(oabi_con,
                 str_remove_all(
                   paste0(
                     "ALTER TABLE [crh_eval].[bband_summary_", 
                     available_dates[date, 2], 
                     "] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)"), 
                   c("\n|\t")))
}
toc()
#
DBI::dbGetQuery(oabi_con, "select count(*) from [crh_eval].[bband_summary_2022_06]")
DBI::dbGetQuery(oabi_con, "select count(*) from [crh_eval].[bband_summary_2022_12]")
