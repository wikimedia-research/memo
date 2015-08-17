# Dependencies
library(parallel) # This comes standard with every R installation
library(wmf) # install.packages("devtools"); devtools::install_github("ironholds/wmf")
library(urltools) # install.packages("urltools")
library(data.table) # install.packages("data.table")
library(ggthemes) # install.packages("ggthemes")
library(ggplot2) # This is installed automatically as a ggthemes dependency.
library(scales)
library(lubridate)

options(scipen = 500)

main <- function(){
  
  # The range for the query. We want January-August, since those are the complete months we have.
  start_date <- "2015-01-01"
  end_date <- "2015-08-02"
  
  # A function for extracting the data we want from each day. This is the proportion of google-sourced
  # pageviews, the /number/ of google-sourced pageviews, the proportion and number of
  # nul-referer pageviews, and the number of pageviews, full-stop.
  memo <- function(filepath){
    
    # Read in the sampled logs for that day and filter them down to just pageviews.
    # Format the timestamps so they're just dates.
    data <- wmf::to_pageviews(wmf::read_sampled_log(filepath))
    data$timestamp <- as.Date(wmf::from_log(data$timestamp))
    
    # Identify pageviews from spiders
    data$is_spider <- wmf::is_spider(data$user_agent)
    
    # Decode referers and identify whether requests are from google. Also identify whether
    # referers are nul (just a dash) so we can count those too.
    data$referer <- urltools::domain(urltools::url_decode(data$referer))
    data$is_google <- grepl(x = data$referer, pattern = "google", fixed = TRUE, useBytes = TRUE)
    data$is_nul <- (data$referer == "-")
    
    # Turn the relevant fields into a data.table so we can aggregate.
    # Then aggregate to just get the number of google pageviews, nul pageviews and pageviews,
    # full stop. We'll distinguish spiders to be on the safe side.
    data <- as.data.table(data[, c("timestamp", "is_spider", "is_nul", "is_google")])
    result <- data[, j = list(google = sum(is_google), no_referer = sum(is_nul),
                              pageviews = .N), by = c("timestamp", "is_spider")]
    gc()
    cat(".")
    return(result)
    
  }
  
  
  # A prototype run showed a 6% variation between sampled and unsampled, JFYI.
  # Get the files
  files <- wmf::get_logfile(earliest = start_date, latest = end_date)
  
  # Run the "memo" process over each file, distributing it over multiple cores so that
  # the end date is some time before the heat-death of the universe.
  results <- parallel::mclapply(X = files, FUN = memo, mc.preschedule = TRUE,
                                mc.cores = 4, mc.cleanup = TRUE)
  
  # Perform a check for errors, and rerun (once!) if errors occurred, with half the cores
  # in case it was a memory/connections issue.
  result_classes <- unlist(lapply(results, function(x){return(class(x)[1])}))
  errored_files <- files[which(result_classes == "try-error")]
  if(length(errored_files)){
    
    #NULL out the errored results
    results <- lapply(X = results, FUN = function(x){
      if(class(x)[1] != "try-error"){
        return(x)
      }
    })
    
    #Rerun to grab the data for the errored-out days.
    rerun_file_data <- parallel::mclapply(X = errored_files, FUN = memo, mc.preschedule = FALSE,
                                          mc.cores = 2, mc.cleanup = TRUE)
    
    #Bind together the (cleaned-up) results and rerun data.
    results <- c(results, rerun_file_data)

  }
  
  # Bind the results together into a single table and check that the date ranges are what we want.
  # Because the log files switch over at 06:00 UTC, not 00:00 UTC, 1 January contains some of 31
  # December, and some of 1 August is in the file for 2 August.
  results <- do.call("rbind", results)
  results <- results[results$timestamp >= as.Date(start_date) & results$timestamp < as.Date(end_date),]
  
  # Multiply up, since all the raw values here are actual_val/1000
  results$google <- results$google*1000
  results$no_referer <- results$no_referer*1000
  results$pageviews <- results$pageviews*1000
  
  # Read in pagecounts from the unsampled logs so we can benchmark and make sure that our
  # overall pageview count is (broadly) accurate before doing further analysis.
  hourly_pagecounts <- wmf::hive_query(query = "
                                     USE wmf;
                                     SELECT project, year, month, day, SUM(view_count) AS pageviews
                                     FROM pageview_hourly
                                     WHERE year = 2015
                                     AND month IN(5,6,7)
                                     GROUP BY project, year, month, day;")
    
  # Clean up by normalising the project name and excluding project types we don't care about
  desired_projects <- c("wikibooks", "wikipedia", "wiktionary", "wikiquote", "wikisource", 
                        "wikinews", "wikiversity", "wikimedia", "wikivoyage", 
                        "wikidata")
  undesired_subgroups <- c("outreach","donate","arbcom-de","arbcom-nl","arbcom-en","arbcom-fi")
  hourly_pagecounts <- hourly_pagecounts[gsub(x = hourly_pagecounts$project, pattern = ".*\\.", replacement = "")
                                         %in% desired_projects,]
  hourly_pagecounts <- hourly_pagecounts[!gsub(x = hourly_pagecounts$project, pattern = "\\..*", replacement = "")
                                         %in% undesired_subgroups,]
  
  # Create dates and aggregate by those
  hourly_pagecounts$timestamp <- as.Date(paste(hourly_pagecounts$year, hourly_pagecounts$month,
                                               hourly_pagecounts$day, sep = "-"))
  hourly_pagecounts <- hourly_pagecounts[, j = list(pageviews = sum(pageviews)), by = c("timestamp")]
  hourly_pagecounts$type <- "Unsampled logs"
  
  # Create results aggregate and bind it in.
  local_results <- results[timestamp %in% hourly_pagecounts$timestamp, j = list(pageviews = sum(pageviews)), by = c("timestamp")]
  local_results$type <- "Sampled logs"
  testing_dataset <- rbind(hourly_pagecounts, local_results)
  
  # Plot
  ggsave(file = "sample_testing.png",
         plot = ggplot(testing_dataset, aes(timestamp, pageviews, group = type, colour = type)) + 
           geom_line(size = 2) + theme_fivethirtyeight() +
           labs(title = "Pageviews by day, sampled versus unsampled logs"))
  
  #Back to the final analysis. Calculate proportions, too.
  results$google_proportion <- results$google/results$pageviews
  results$nul_proportion <- results$no_referer/results$pageviews
  
  #Add a month column to make aggregates really easy.
  results$month <- results$timestamp
  lubridate::day(results$month) <- 1
  
  # Look at google proportions, minus spiders
  ggsave(file = "google_proportions.png",
         plot = ggplot(results[results$is_spider == FALSE,], aes(timestamp, google_proportion)) + 
           geom_line(colour = "#CC0000") + theme_fivethirtyeight() + stat_smooth() +
           scale_x_date() + scale_y_continuous(labels = percent) +
           labs(x = "Date", y = "Pageviews (%)", title = "Proportion of pageviews with Google referers"))

  ggsave(file = "nul_proportions.png",
         plot = ggplot(results[results$is_spider == FALSE,], aes(timestamp, nul_proportion)) + 
           geom_line(colour = "#CC0000") + theme_fivethirtyeight() + stat_smooth() +
           scale_x_date() + scale_y_continuous(labels = percent) +
           labs(x = "Date", y = "Pageviews (%)", title = "Proportion of pageviews with no referers"))
  
}
