## Clean data functions --------------------------------------------------------
# Function to extract characters
extract_char = function(data){
  result = regmatches(data %>% unlist, 
                      gregexpr("[[:alpha:]]+",
                               data %>% unlist
                      )
  ) %>%
    future_map(
      ~paste0(.,
              collapse = ""
      )
    ) %>% 
    unlist
  return(result)
}

# Function to replace latin letters to english letters
replace_latin = function(data){
  lat_1 <- "šžþàáâãäåçèéêëìíîïðñòóôõöùúûüýŠŽÞÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝ"
  eng_1 <- "szyaaaaaaceeeeiiiidnooooouuuuySZYAAAAAACEEEEIIIIDNOOOOOUUUUY"
  lat_2 <- c("œ", "ß", "æ", "ø", "Œ", "ß", "Æ", "Ø")
  eng_2 <- c("oe", "ss", "ae", "oe", "OE", "SS", "AE", "OE")
  result = data %>%
    unlist %>% 
    chartr(lat_1, eng_1, .)
  for (i in 1:length(lat_2)) {
    result = gsub(lat_2[i], eng_2[i], result, fixed = TRUE)
  }
  return(result)
}

# Function to replace "" with NA
replace_NA_char = function (data){
  if (class(data) == "character") {
    data = case_when(
      grepl("^\\s*$",data) ~ NA_character_,
      TRUE ~ data
    )
  }
  return(data)
}

# Function to clean data
clean_ca_data = function(data){
  print("Cleaning data...")
  data_clean = data %>% 
    mutate(
      LastName = LastName %>% replace_latin,
      FirstName = FirstName %>% replace_latin,
      MiddleName = MiddleName %>% replace_latin,
      Suffix = Suffix %>% replace_latin,
      
      LastName_char = LastName %>% extract_char %>% tolower %>% replace_NA_char,
      FirstName_char = FirstName %>% extract_char %>% tolower %>% replace_NA_char,
      MiddleName_char = MiddleName %>% extract_char %>% tolower,
      Suffix_char = Suffix %>% extract_char %>% tolower %>% replace_NA_char,
      
      LastName_char1 = LastName_char %>% substr(., 1, 1),
      FirstName_char1 = FirstName_char %>% substr(., 1, 1),
      MiddleName_char1 = MiddleName_char %>% substr(., 1, 1) %>% replace_NA_char,
      Suffix_char1 = Suffix_char %>% substr(., 1, 1),
      
      DOB_year = DOB %>% substr(., 1, 4) %>% replace_NA_char,
      DOB_month = DOB %>% substr(., 6, 7) %>% replace_NA_char,
      DOB_day = DOB %>% substr(., 9, 10) %>% replace_NA_char,
      
      Zip_5digit = Zip %>% substr(., 1, 5) %>% replace_NA_char,
      Initials = paste0(FirstName_char1, LastName_char1) %>% replace_NA_char
    )
  print("Data are cleaned.")
  
  file_name = paste0(path, clean_path, file_code, "_data_clean.Rds")
  saveRDS(data_clean, file = file_name)
  print(paste0(file_name, " is saved."))
  
  return(data_clean)
}


## Not Run -------------------------------------------------------------------------
# plan(multisession, workers = 6)
# data = clean_ca_data(data)




