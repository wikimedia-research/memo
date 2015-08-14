# Dependencies
library(parallel)
library(wmf)
library(urltools)
library(data.table)

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
    
    # Decode referers and identify whether requests are from google.
    data$referer <- urltools::domain(urltools::url_decode(data$referer))
    data$is_google <- grepl(x = data$referer, pattern = "google", fixed = TRUE, useBytes = TRUE)
    data$is_nul <- (data$referer == "-")
    
    # Turn the relevant fields into a data.table so we can aggregate.
    # Then aggregate to just get the number of google pageviews, nul pageviews and pageviews,
    # full stop.
    data <- as.data.table(data[, c("timestamp", "is_spider", "is_nul", "is_google")])
    result <- data[, j = list(google = sum(is_google), no_referer = sum(is_nul),
                              pageviews = .N), by = "timestamp"]
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
    rerun_file_data <- parallel::mclapply(X = errored_files, FUN = memo, mc.preschedule = FALSE,
                                          mc.cores = 2, mc.cleanup = TRUE)
    test <- c(results, rerun_file_data)
  }
  
  # Bind the results together into a single table and check that the date ranges are what we want.
  # Because the log files switch over at 06:00 UTC, not 00:00 UTC, 1 January contains some of 31
  # December, and some of 1 August is in the file for 2 August.
  results <- do.call("rbind", results)
  results <- results[results$timestamp >= as.Date(start_date) & results$timestamp <= as.Date(end_date),]
  
}
