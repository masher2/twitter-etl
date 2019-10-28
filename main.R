#! /usr/bin/Rscript
suppressMessages({
  library(optparse)
  library(DBI)
  library(RSQLite)
  library(rtweet)
  library(dplyr)
  library(stringr)
  library(tm)
})


# Functions -------------------------------------------------------------------
#' Set up the database
#'
#' Given a name contruct the database to hold the tweets data.
setup_database <- function(database = "tweets.db") {
  if (file.exists(database)) {
    database <- paste(format(Sys.time(), "%Y%m%d_%H%M%S"), database, sep = "_")
  }
  conn <- DBI::dbConnect(RSQLite::SQLite(), database)
  DBI::dbExecute(
    conn,
    "CREATE TABLE tweet_data(
      tweet_id INTEGER PRIMARY KEY,
      date_created INTEGER,
      user TEXT,
      content TEXT,
      source TEXT,
      location TEXT,
      quoted_user TEXT,
      quoted_content TEXT
    )"
  )
  DBI::dbDisconnect(conn)
}


#' Get tweets for a set of keys
#'
#' Stream the important tweets into a json file and then return the name of the
#' file to use it in the next steps of the ETL
get_tweets <- function(keys, timeout = 600, raw_data_dir = "raw_data") {
  if (!dir.exists(raw_data_dir)) dir.create(raw_data_dir) 

  filename <- file.path(
    raw_data_dir,
    paste0("stream_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json")
  )
  rtweet::stream_tweets(
    q = keys,
    timeout = timeout,
    parse = FALSE,
    file_name = filename,
    verbose = FALSE
  )

  filename
}


#' Process the tweets into a dataframe
transform_tweets <- function(filename) {
  if (file.size(filename) == 0) return(NULL)

  df <- rtweet::parse_stream(filename, verbose=FALSE)
  df <- dplyr::filter(df, !is_retweet, lang == "es")
  df <- dplyr::transmute(
    df,
    date_created = created_at,
    user = screen_name,
    content = text,
    source = source,
    location = location,
    quoted_user = quoted_screen_name,
    quoted_content = quoted_text
  )
  df <- dplyr::mutate_at(
    df,
    vars(content, quoted_content),
    function(text) {
      text = text %>% 
        stringr::str_to_lower() %>%
        stringr::str_remove_all("\\s?(f|ht)(tp)(s?)(://)([^\\.]*)[\\.|/](\\S*)") %>%
        stringr::str_remove_all("@\\w+") %>%
        tm::removeWords(tm::stopwords("spanish")) %>%
        stringr::str_squish()
    }
  )
  df <- dplyr::filter(df, !duplicated(content))

  df
}


#' Load the tweets into a SQLite database
load_tweets <- function(tweets, database = "tweets.db") {
  if (is.null(tweets)) return()

  conn <- DBI::dbConnect(RSQLite::SQLite(), database)
  DBI::dbWriteTable(conn, "tweet_data", tweets, append = TRUE)
  DBI::dbDisconnect(conn)
}


# CLI Options -----------------------------------------------------------------
option_list <- list(
  make_option(
    c("--database"),
    type = "character",
    default = "tweets.db",
    help = "Choose the name of the sqlite database file. [default %default]"
  ),
  make_option(
    c("--keys"),
    type = "character",
    default = "#chile,#chiledesperto,#santiago",
    help = "Search queries for twitter, separated by commas."
  ),
  make_option(
    c("--initial-setup"),
    action = "store_true",
    default = FALSE,
    help = "Create a new sqlite database to store the tweets in."
  ),
  make_option(
    c("--raw-data-dir"),
    type = "character",
    default = "raw_data",
    help = "Directory to store the raw streams in. [default %default]"
  ),
  make_option(
    c("-c", "--stream-chunks"),
    type = "integer",
    default = 5,
    help = "How many tweet-streaming calls to do in the duration of the script. [default %default]"
  ),
  make_option(
    c("-t", "--stream-timeout"),
    type = "integer",
    default = 60,
    help = "Duration of the tweet-streaming calls, in seconds. [default %default]"
  )
)
option_parser <- OptionParser(option_list = option_list)
opt <- parse_args(option_parser, convert_hyphens_to_underscores = TRUE)

# Program ---------------------------------------------------------------------
if (opt$initial_setup) {
  setup_database(opt$database)
}

token <- rtweet::create_token(
  app = 'app',
  consumer_key = Sys.getenv('TW_CONSUMER_KEY'),
  consumer_secret = Sys.getenv('TW_CONSUMER_SECRET'),
  access_token = Sys.getenv('TW_ACCESS_TOKEN'),
  access_secret = Sys.getenv('TW_ACCESS_SECRET')
)

for (i in 1:opt$stream_chunks) {
  tweet_file <- get_tweets(
    opt$keys,
    timeout = opt$stream_timeout,
    raw_data_dir = opt$raw_data_dir
  )
  tweets <- transform_tweets(tweet_file)
  load_tweets(tweets, database = opt$database)
}
