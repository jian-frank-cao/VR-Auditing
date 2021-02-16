## read file functions ---------------------------------------------------------
# Function to read file
read_file = function(file_name, sep, quote, fileEncoding, error_rows){
  data = NULL
  
  count = 1
  data[["header-1"]] = read.csv(file = file_name,
                                header = FALSE,
                                skip = count - 1,
                                nrows = 1,
                                sep = sep,
                                quote = quote,
                                fileEncoding = fileEncoding,
                                stringsAsFactors = FALSE
  )
  print("Header is read.")
  for (i in 1:length(error_rows)) {
    data[[paste0(count + 1, "-", error_rows[i] - 1)]] = read.csv(file = file_name,
                                                                 header = FALSE,
                                                                 skip = count,
                                                                 nrows = error_rows[i] - 1 - count,
                                                                 sep = sep,
                                                                 quote = quote,
                                                                 fileEncoding = fileEncoding,
                                                                 stringsAsFactors = FALSE
    )
    print(paste0(count + 1, "-", error_rows[i] - 1," is read."))
    count = error_rows[i] - 1
    data[[paste0("error", "-", error_rows[i])]] = read.csv(file = file_name,
                                                           header = FALSE,
                                                           skip = count,
                                                           nrows = 1,
                                                           sep = sep,
                                                           quote = quote,
                                                           fileEncoding = fileEncoding,
                                                           stringsAsFactors = FALSE
    )
    print(paste0("error", "-", error_rows[i]," is read."))
    count = error_rows[i]
  }
  data[[paste0(count + 1, "-", "end")]] = read.csv(file = file_name,
                                                   header = FALSE,
                                                   skip = count,
                                                   sep = sep,
                                                   quote = quote,
                                                   fileEncoding = fileEncoding,
                                                   stringsAsFactors = FALSE
  )
  print(paste0(count + 1, "-", "end is read."))
  n = data %>% lapply(., nrow) %>% do.call("rbind",.) %>% sum
  cat(paste0("Number of lines in the file: \n"))
  system(paste0("wc -l ", file_name))
  cat("Number of lines has been read: \n")
  cat(paste0(n, "\n"))
  return(data)
}

# Function to correct the class
correct_class = function(data, target_class){
  for (i in 1:length(target_class)) {
    if (target_class[i]=="character") {
      data[,i] = as.character(data[,i])
    }else if(target_class[i]=="integer"){
      data[,i] = as.integer((data[,i]))
    }else if(target_class[i]=="factor"){
      data[,i] = as.factor((data[,i]))
    }else{
      print(paste0("Column ",i," is ",target_class[i]))
    }
  }
  return(data)
}

# Function to detect error rows
detect_error_rows = function(file_name, n_fields = 56){
  count_fields = count.fields(
    file_name,
    sep = "\t",
    quote = ""
  )
  return(which(count_fields > n_fields))
}

# Function to read ca data
read_ca_data = function(file_code, file_vrd, error_ids){
  
  file_name = paste0(path, txt_path, file_vrd)
  
  # detect error rows
  error_rows = detect_error_rows(
    paste0(path,txt_path,file_vrd)
  )
  
  if (length(error_rows) == 0) {
    # read data without error
    data_vrd = read.csv(file = file_name,
                        header = TRUE,
                        sep = "\t",
                        quote = "",
                        fileEncoding = "latin1",
                        stringsAsFactors = FALSE
    )
    
  }else{
    # read data with errors
    data = read_file(file_name = file_name,
                     sep = "\t",
                     quote = "",
                     fileEncoding = "latin1",
                     error_rows = error_rows
    )
    
    # handle errors
    n_rows = sapply(data, nrow)
    max_rows = which(n_rows == max(n_rows))
    col_class = sapply(data[[max_rows]],class)
    
    for (i in 1:length(error_rows)) {
      ind = i*2 + 1
      if ((data[[ind]][1, 2] %in% error_ids) & 
          (data[[ind]][1, 8] %>% is.na)) {
        data[[ind]] = data[[ind]][1,-8] %>%
          `colnames<-`(colnames(data[[max_rows]]))
      }else{
        print(data[[ind]])
        fields = readline(prompt = "Enter column number(s): ")
        fields = regmatches(fields, 
                            gregexpr("[[:digit:]]+",
                                     fields
                            ))[[1]] %>% as.numeric
        data[[ind]] = data[[ind]][1,-fields] %>%
          `colnames<-`(colnames(data[[max_rows]]))
      }
    }
    
    for (i in 2:length(data)) {
      if (i != max_rows) {
        data[[i]] = correct_class(data[[i]], col_class)
      }
    }
    
    # concat data blocks
    data_vrd = data[2:length(data)] %>%
      do.call("rbind",.)
    
    data_vrd = data_vrd %>%
      `colnames<-`(data[[1]][1,] %>% unlist) %>%
      `rownames<-`(1:nrow(data_vrd))
    
  }
  
  # check nrows
  system_output = system(paste0("wc -l ",paste0(path, txt_path, file_vrd)),
                         intern = TRUE)
  file_rows = regmatches(system_output, 
                         gregexpr("[[:digit:]]+",
                                  system_output
                         ))[[1]][1] %>% as.numeric
  
  print(paste0("All rows have been read ---------- ",
             file_rows == nrow(data_vrd) + 1))
  
  # save data
  saveRDS(
    data_vrd,
    paste0(
      path,
      raw_path,
      file_code,
      "_data_vrd_latin1.Rds"
    )
  )
  
  print(paste0(file_code,
             "_data_vrd_latin1.Rds is saved"
  ))
  
  return(data_vrd)
}

  
## Run -------------------------------------------------------------------------
error_ids = c(38359411, 38362837, 38359739)
file_code = "1316"
file_vrd = '1316-90617-59-pvrdr-vrd-20210201-1415.TXT'


data = read_ca_data(file_code, file_vrd, error_ids)
