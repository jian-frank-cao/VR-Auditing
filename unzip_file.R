## unzip functions -------------------------------------------------------------
# Function to unzip vrd files
unzip_ca_files = function(source_path, dest_path, target = "vrd"){
  # list zip files
  files_list = list.files(pattern = ".zipx", path = source_path, all.files = TRUE)
  
  # type in passwords
  passwords = rep("", length(files_list))
  for (i in 1:length(files_list)) {
    passwords[i] = readline(prompt = paste0("Enter the password (",
                                           files_list[i], "): "))
  }
  
  # unzip vrd files
  job = NULL
  for (i in 1:length(files_list)) {
    file = files_list[i]
    password = passwords[i]
    job[[file]] = NULL
    content = unzip(
      paste0(
        source_path,
        file
      ),
      list = TRUE
    )
    
    for (item in content$Name) {
      if (!grepl(paste0("pvrdr-", target), item)) {
        next
      }
      job[[file]]$file_vrd = item
      job[[file]]$file_code = regmatches(item, 
                                         gregexpr("[[:digit:]]+",
                                                  item
                                         ))[[1]][1] %>% as.numeric
      print(paste0("Unzipping ", item))
      
      system(
        paste0(
          "7z x '-i!",
          item,
          "' ",
          source_path,
          file,
          " -o",
          dest_path,
          " -p",
          password
        )
      )
    }
  }
  return(job)
}

## Run -------------------------------------------------------------------------
job = unzip_ca_files(paste0(path, zip_path),
                     paste0(path, txt_path),
                     "pd")

