## Save dups functions -------------------------------------------------------------
# Function that identifies the dups
identify_dups = function(data){
  df = data %>%
    select(c("RegistrantID", "ID_unique")) %>%
    # mutate(
    #   RegistrationDate = as.Date(RegistrationDate, "%Y-%m-%d")
    # ) %>%
    group_by(ID_unique) %>%
    mutate(
      is_dup = case_when(
        RegistrantID == max(RegistrantID) ~ TRUE,
        TRUE ~ TRUE
      )
    ) %>%
    ungroup
  data$is_dup = df$is_dup
  return(data)
}

# Function that groups the duplicates
group_dups = function(ind){
  matches = NULL
  count = 1
  all_finish = FALSE
  while (all_finish == FALSE) {
    set = ind[1,] %>% unlist
    ind = ind[-1,]
    target_finish = FALSE
    while (target_finish == FALSE) {
      check_a = which(ind$ID_A %in% set)
      check_b = which(ind$ID_B %in% set)
      goal = c(check_a, check_b) %>% unique
      if (length(goal) == 0) {
        target_finish = TRUE
      }else{
        set = c(set, ind[goal,] %>% unlist) %>% unique
        ind = ind[-goal,]
      }
    }
    matches[[count]] = set
    count = count + 1
    if (nrow(ind) == 0) {
      all_finish = TRUE
    }
  }
  return(matches)
}

# Function that filter the matched dups with prob
filter_prob_dups = function(df, prob){
  filtered = future_map2(
    df$dups_ind,
    df$dups,
    ~ {
      dups_ind = .x
      data = .y
      dups_ind = dups_ind %>%
        filter(Prob >= prob)
      ind = dups_ind %>%
        .[c("ID_A","ID_B")] %>%
        unlist %>%
        unique
      out = data[match(ind, data$ID),]
      out = out %>%
        mutate(
          ID_unique = ID
        )
      matches = group_dups(dups_ind[c("ID_A","ID_B")])
      for (i in 1:length(matches)) {
        out$ID_unique[out$ID_unique %in% matches[[i]]] = matches[[i]][1]
      }
      out = identify_dups(out)
    }
  )
  return(filtered)
}


