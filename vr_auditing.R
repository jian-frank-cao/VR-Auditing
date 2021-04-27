## Set environment -------------------------------------------------------------
library(tidyverse)
library(data.table)
library(furrr)
options(future.globals.maxSize= 89128960000)

path = "F:/MyFiles/Documents/GitHub/VR-Auditing/" # ends with /
setwd(path)

data_path = "data/"
txt_path = "data/txt/"
raw_path = "data/raw/"
clean_path = "data/clean/"
subset_root_path = "data/subset/"
subset_path = "data/subset/initials/"
result_path = "data/result/"
exact_path = "data/result/exact/"
stats_path = "data/result/stats/"
prob_path = "data/result/prob/"

folders = c(data_path, txt_path, raw_path, clean_path, subset_root_path,
            subset_path, result_path, stats_path, exact_path, prob_path)

for (folder in folders) {
  dir.create(
    paste0(
      path,
      folder
    ),
    showWarnings = FALSE
  )
}

## Load Functions -------------------------------------------------------------
function_files = list.files(path = "./functions/", pattern = ".R")

for (function_file in function_files) {
  source(paste0("./functions/", function_file))
}


## Unzip File (optional) ------------------------------------------------------
# jobs = unzip_ca_files(paste0(path, zip_path),
#                      paste0(path, txt_path),
#                     "-vrd-")


## Read File ------------------------------------------------------------------
file_vrd = "1316-90617-59-pvrdr-vrd-20210201-1415.TXT"
file_code = "1316"
error_ids = c(38359411, 38362837, 38359739)

data = read_ca_data(file_code, file_vrd, error_ids)


## Clean Data -----------------------------------------------------------------
plan(multiprocess, workers = 3) # adjust to avoid bursting memory
data = clean_ca_data(data)


## Save Stats (optional) ------------------------------------------------------
# save_ca_stats(data, file_code)


## Exact Matching (optional) --------------------------------------------------
# design_exact = list(
#   var_list = c("LastName_char","FirstName_char","DOB",
#                "MiddleName_char1","Suffix_char"),
#   var_string = c("LastName_char","FirstName_char","DOB",
#                  "MiddleName_char1","Suffix_char"),
#   var_numeric = NULL
# )
#
# dedup_ca_exact(data, design_exact)


## Assign Jobs ----------------------------------------------------------------
plan(multiprocess, workers = 12) # set to maximum CPU cores
assign_ca_jobs(data)


## Probabilistic Matching -----------------------------------------------------
plan(multiprocess, workers = 12) # set to maximum CPU cores
match_n = 2

# Variable list
design_prob = list(
  match_1 = list(
    var_list = c("LastName","FirstName","DOB",
                 "MiddleName_char1","Suffix_char"),
    var_string = c("LastName","FirstName","DOB",
                   "MiddleName_char1","Suffix_char"),
    var_numeric = NULL
  ),
  match_2 = list(
    var_list = c("LastName_char","FirstName_char","DOB",
                 "MiddleName_char1","Suffix_char"),
    var_string = c("LastName_char","FirstName_char","DOB",
                   "MiddleName_char1","Suffix_char"),
    var_numeric = NULL
  )
)

# get batches
batches = readRDS(
  file = paste0(
    path,
    subset_path,
    file_code,
    "/batches/",
    file_code,
    "_batches.Rds"
  )
)

# match
output = batches %>%
  set_names(names(batches)) %>%
  future_map(
    ~{
      batch = .
      data = readRDS(
        paste0(
          path,
          subset_path,
          file_code,
          "/",
          batch$file
        )
      )%>%
        select(design_prob[[match_n]][["var_list"]])
      output = list(
        dups = dedup_pbl(data = data,
                         batches = batch[c("ind.a","ind.b")] %>% list,
                         var_list = design_prob[[match_n]][["var_list"]],
                         var_string = design_prob[[match_n]][["var_string"]],
                         var_numeric = design_prob[[match_n]][["var_numeric"]],
                         threshold_match = 0.95),
        batch = batch
      )

    }
  )

result = combine_dups(output)
dups = extract_prob_dups(result$dups_ind)

saveRDS(
  list(
    output = output,
    dups_ind = result$dups_ind,
    patterns = result$patterns,
    dups = dups,
    design = design_prob[[match_n]]
  ),
  file = paste0(
    path,
    prob_path,
    file_code,
    "_prob_match_initials_",
    match_n,
    ".Rds"
  )
)


## Save registrantID for Sean ------------------------------------------------
result = readRDS(
  file = paste0(
    path,
    prob_path,
    file_code,
    "_prob_match_initials_2.Rds"
  )
)

output = result$dups_ind %>%
  names %>%
  as.list %>%
  future_map(
    ~ {
      target = .
      id = result$dups_ind[[target]]
      dups = result$dups[[target]]
      patterns = result$patterns[[target]]
      ind = (id$Prob > 0.995) %>% which
      RegistrantID = cbind(
        dups$RegistrantID[match(id$ID_A, dups$ID)],
        dups$RegistrantID[match(id$ID_B, dups$ID)]
      )
      out = cbind(
        RegistrantID,
        patterns
      ) %>%
        `colnames<-`(c("RegistrantID_A", "RegistrantID_B",
                       "LastName_char","FirstName_char", "DOB",
                       "MiddleName_char1", "Suffix_char")) %>%
        mutate(
          LastName_char = case_when(
            is.na(LastName_char) ~ 0,
            TRUE ~ LastName_char/2
          ),
          FirstName_char = case_when(
            is.na(FirstName_char) ~ 0,
            TRUE ~ FirstName_char/2
          ),
          DOB = case_when(
            is.na(DOB) ~ 0,
            TRUE ~ DOB/2
          ),
          MiddleName_char1 = case_when(
            is.na(MiddleName_char1) ~ 0,
            TRUE ~ MiddleName_char1/2
          ),
          Suffix_char = case_when(
            is.na(Suffix_char) ~ 0,
            TRUE ~ Suffix_char/2
          )
        )
      out[ind, ]
    }
  ) %>%
  do.call("rbind", .)

extract_date = result[["dups"]][[1]][["ExtractDate"]][1]

write.csv(output,
          paste0("./data/RegistrantID_probabilistic_matching_",
                 extract_date, ".csv"),
                 row.names = FALSE)


## Save potential duplicates for Sam -----------------------------------------
prob = 0.995

data_filtered = filter_prob_dups(result, prob)

data_filtered = future_map2(
  data_filtered,
  1:length(data_filtered) * 1000000,
  ~{
    data = .x
    addon = .y
    data = data %>%
      mutate(
        DupsGroup = ID_unique + addon
      ) %>%
      select(-c("ID", "ID_unique", "is_dup"))
  }
) %>%
  do.call("rbind", .) %>%
  arrange(DupsGroup)


write.table(
  data_filtered,
  file = paste0("./data/Potential_Duplicates_Probabilistic_matching_",
                extract_date,".txt"),
  row.names = FALSE,
  sep = "\t",
  quote = FALSE,
  fileEncoding = "latin1"
)


