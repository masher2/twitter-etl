# Main ETL file
library(rtweet)


#' Get tweets for a set of keys
#'
#' Stream the important tweets into a json file and then return the name of the
#' file to use it in the next steps of the ETL
get_tweets <- function(keys) {
  query <- paste(keys, collapse=",")
  filename <- paste0("stream", format(Sys.Time(), "%Y%m%d_%H%M%S%"), ".json")
  stream_tweets(q = query, timeout = 600, file_name = filename)

  filename
}

#' Process the tweets into a dataframe
transform_tweets <- function(filename) {
}

#' Load the tweets into a SQLite database
load_tweets <- function(tweets) {
}


# Main loop ----
token <- create_token(
  app = 'app',
  consumer_key = Sys.getenv('TW_CONSUMER_KEY'),
  consumer_secret = Sys.getenv('TW_CONSUMER_SECRET'),
  access_token = Sys.getenv('TW_ACCESS_TOKEN'),
  acces_secret = Sys.getenv('TW_ACCESS_SECRET')
)
keys <- c('#chile', '#chiledesperto', '#santiago')

counter <- 0

while(counter <= 6) {
  tweet_file <- get_tweets(keys)
  counter <- counter + 1
}
