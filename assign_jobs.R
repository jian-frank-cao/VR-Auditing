## Split subsets functions -----------------------------------------------------
# Function to split data into subsets accroding to target variable
split_subset = function(data, target, by_size=FALSE, subset_size=100000){
  if (by_size) {
    # count frequency
    count_target = data %>% 
      select(all_of(target)) %>% 
      table %>% 
      data.frame %>% 
      `colnames<-`(c(target,"Count")) %>% 
      .[order(.[1]),]
    
    # define groups
    count_target = count_target %>% 
      mutate(
        group = ""
      )
    
    n_record = 0
    n_group = 1
    for (i in 1:nrow(count_target)) {
      n_record = n_record + count_target$Count[i]
      count_target$group[i] = paste0("Group-",n_group)
      if (n_record >= subset_size) {
        n_record = 0
        n_group = n_group + 1
      }
    }
    
    # map to data
    data = full_join(data, count_target[c(1,3)], by = target)
    
    # split data into list of subsets
    result = data %>% 
      group_by(group) %>%
      group_split(
        .,
        keep = FALSE
      )
  }else{
    # target = enquo(target)
    # split data into list of subsets
    result = data %>% 
      group_by_at(vars(one_of(target))) %>% 
      group_split(
        .,
        keep = FALSE
      )
  }
  if (result %>% .[[length(result)]] %>% nrow < 100) {
    result[[length(result) - 1]] = rbind(
      result[[length(result) - 1]],
      result[[length(result)]]
    )
    result[[length(result)]] = NULL
  }
  return(result)
}

# Function to save subsets into rds files
save_rds = function(df_list,path){
  for (i in 1:length(df_list)) {
    saveRDS(
      df_list[[i]],
      paste0(
        path,
        "Group-",
        i,
        ".Rds"
      )
    )
  }
}

save_rds_future = function(df_list,subset_path){
  future_map2(df_list,
              paste0("Group-",
                     1:length(df_list)),
              ~saveRDS(.x,
                       file = paste0(subset_path,
                                     .y,
                                     ".Rds")))
}

# Function to split batches
split_batch = function(data, batch_size = 10000){
  batches = NULL
  n_a = ceiling(nrow(data)/batch_size)
  size_a = ceiling(nrow(data)/n_a)
  for (i in 1:n_a) {
    if (i != n_a) {
      batches[[i]] = list(
        ind.a = ((i-1)*size_a+1):(i*size_a),
        ind.b = ((i-1)*size_a+1):nrow(data)
      )
    }else{
      batches[[i]] = list(
        ind.a = ((i-1)*size_a+1):nrow(data),
        ind.b = ((i-1)*size_a+1):nrow(data)
      )
    }
  }
  return(batches)
}

assign_ca_jobs = function(data){
  # split subsets
  print("Splitting subsets...")
  df_list = split_subset(
    data = data,
    target = "Initials",
    by_size = TRUE,
    subset_size=100000
  )
  
  print("Saving subsets...")
  dir.create(
    paste0(
      path,
      subset_path,
      file_code
    ),
    showWarnings = FALSE
  )
  
  response = save_rds_future(
    df_list,
    paste0(
      path,
      subset_path,
      file_code,
      "/"
    )
  )
  print("Subsets are saved.")
  
  # split batches
  print("Splitting batches...")
  batches = df_list %>% 
    set_names(
      paste0(
        "Group-",
        1:length(df_list)
      )
    ) %>% 
    future_map(
      ~ split_batch(.)
    )
  
  for (i in 1:length(batches)) {
    names(batches[[i]]) = paste0(
      1:length(batches[[i]])
    )
    for (j in 1:length(batches[[i]])) {
      batches[[i]][[j]]$file = paste0(
        names(batches)[i],
        ".Rds"
      )
    }
  }
  
  batches = batches %>% 
    do.call("c",.)
  
  print("Saving batches...")
  dir.create(
    paste0(
      path,
      subset_path,
      file_code,
      "/batches"
    ),
    showWarnings = FALSE
  )
  
  saveRDS(
    batches,
    file = paste0(
      path,
      subset_path,
      file_code,
      "/batches/",
      file_code,
      "_batches.Rds"
    )
  )
  print("Batches are saved.")
}


## Run -------------------------------------------------------------------------
plan(multisession, workers = 30)
assign_ca_jobs(data)

