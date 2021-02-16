## exact matching functions ----------------------------------------------------
# Function for exact matching
dedup_exact = function(data, var_list){
  # create id and unique id
  data = data %>% 
    mutate(
      ID = 1:nrow(data),
      ID_unique = 1:nrow(data)
    )
  print("IDs are created.")
  
  # find exact duplicates
  duplicated_asc = duplicated(data %>%
                                select(all_of(var_list))
  )
  print("duplicated_asc is finished.")
  
  duplicated_desc = duplicated(data %>%
                                 select(all_of(var_list)),
                               fromLast = TRUE
  )
  print("duplicated_desc is finished.")
  
  ind_dup = which(duplicated_asc | duplicated_desc)
  print("Exact dups are found.")
  
  # find representatives of dup pairs
  ind_rep = which(!duplicated_asc & duplicated_desc)
  print("Exact reps are found.")
  
  dups = NULL
  if (length(ind_dup)>0) {
    # extract dups
    dups = data[ind_dup,]
    print("Dups are extracted.")
    
    # extract reps
    reps = data[ind_rep,]
    print("Reps are extracted.")
    
    # use rep's ID_unique to represent the dup pair
    ind_ID = prodlim::row.match(dups[var_list],reps[var_list])
    dups$ID_unique = reps$ID_unique[ind_ID]
    print("ID_unique are corrected.")
    
    # order the dups
    dups = dups %>% 
      arrange(ID_unique)
    print("Dups are rearranged.")
  }
  print("Done.")
  return(dups)
}

dedup_ca_exact = function(data, design_exact){
  print("Start deduplication using exact matching...")
  dups = dedup_exact(
    data = data,
    var_list = design_exact[["var_list"]]
  )
  saveRDS(list(dups = dups,
               design = design_exact,
               nrow = nrow(data)),
          file = paste0(path,
                        exact_path,
                        file_code,
                        "_exact_match_2",
                        ".Rds"))
  print(
    paste0(path,
           exact_path,
           file_code,
           "_exact_match_2",
           ".Rds is saved.")
  )
}

## Run -------------------------------------------------------------------------
# design
design_exact = list(
  var_list = c("LastName_char","FirstName_char","DOB",
               "MiddleName_char1","Suffix_char"),
  var_string = c("LastName_char","FirstName_char","DOB",
                 "MiddleName_char1","Suffix_char"),
  var_numeric = NULL
)

dedup_ca_exact(data, design_exact)



