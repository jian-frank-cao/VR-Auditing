## Save stats functions -------------------------------------------------------------
# Function to save summary report
save_summary = function(data){
  summary_file <- file(paste0(path,
                          summary_path,
                          strsplit(file_vrd, ".", fixed = TRUE)[[1]][1],
                          "-summary.txt"),
                   open = "wt")
  sink(summary_file)
  psych::describe(data)
  sink()
}

# Function to count the voters
count_voter = function(data, target){
  output = data %>% 
    select(all_of(target)) %>% 
    table %>% 
    data.frame %>% 
    `colnames<-`(c(target,"Count")) %>% 
    group_by_at(vars(all_of(target))) %>% 
    summarise(
      Count = sum(Count)
    ) %>% 
    data.frame %>% 
    arrange_at(vars(all_of(target))) %>% 
    mutate(
      Percent = Count/nrow(data)
    )
  return(output)
}

# Function to save voter stats
save_ca_stats = function(data){
  # Read county_code.csv
  county_code = read.csv(
    file = paste0(
      path,
      data_path,
      "CA_CountyCode.csv"
    )
  )
  
  # var list
  var_list = c(
    "CountyCode", "City", "Zip", "Gender", "PartyCode",
    "Status", "RegistrationDate", "Precinct", "PrecinctNumber",
    "RegistrationMethodCode", "PrecinctId"
  )
  
  # Count voters
  voters_count = var_list %>% 
    as.list %>% 
    set_names(var_list) %>% 
    future_map(
      ~ count_voter(data, .)
    )
  
  # process data
  voters_count$CountyCode = voters_count$CountyCode %>% 
    mutate(
      County = county_code$County
    )
  
  voters_count$RegistrationDate = voters_count$RegistrationDate %>% 
    mutate(
      RegistrationDate = as.Date(RegistrationDate)
    )
  
  # Save stats
  saveRDS(
    list(
      by_group = voters_count,
      n_voters = nrow(data)
    ),
    file = paste0(
      path,
      stats_path,
      file_code,
      "_stats.Rds"
    )
  )
  print(paste0("Stats of snapshot ", file_code, " are saved."))
}

## Run -------------------------------------------------------------------------
save_ca_stats(data, file_code)

