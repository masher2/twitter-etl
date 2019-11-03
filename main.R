#! /usr/bin/Rscript
suppressMessages({
  library(optparse)
  library(futile.logger)
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
  flog.info("Setting up database '%s'", database)

  if (file.exists(database)) {
    flog.info("file '%s' already exist, chosing new name for database file", database)
    database <- paste(format(Sys.time(), "%Y%m%d_%H%M%S"), database, sep = "_")
    flog.info("database file name: %s", database)
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
  flog.info("database '%s' created", database)
}


#' Get tweets for a set of keys
#'
#' Stream the important tweets into a json file and then return the name of the
#' file to use it in the next steps of the ETL
get_tweets <- function(keys, timeout = 600, raw_data_dir = "raw_data") {
  if (!dir.exists(raw_data_dir)) {
    flog.info("Creating folder '%s' to store the raw data streams", raw_data_dir)
    dir.create(raw_data_dir) 
  }

  filename <- file.path(
    raw_data_dir,
    paste0("stream_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json")
  )
  flog.debug("Streaming tweets into %s for %s seconds", filename, timeout)
  rtweet::stream_tweets(
    q = keys,
    timeout = timeout,
    parse = FALSE,
    file_name = filename,
    verbose = TRUE
  )
  flog.debug("Stream completed")

  filename
}


#' Process the tweets into a dataframe
transform_tweets <- function(filename) {
  if (file.size(filename) == 0) {
    flog.warn("File %s is empty", filename)
    return(NULL)
  }

  flog.info("Formatting tweets")
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
  flog.info("Tweets formatted")

  df
}


#' Load the tweets into a SQLite database
load_tweets <- function(tweets, database = "tweets.db") {
  if (is.null(tweets)) return()

  flog.info("Loading tweets to database")
  conn <- DBI::dbConnect(RSQLite::SQLite(), database)
  DBI::dbWriteTable(conn, "tweet_data", tweets, append = TRUE)
  DBI::dbDisconnect(conn)
  flog.info("Tweets loaded")
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
    help = 
"Create a new sqlite database to store the tweets in.
When this flag is used only the database is created, the streaming is not done
unless the flag `--force-stream` is used also."
  ),
  make_option(
    c("-f", "--force-stream"),
    action = "store_true",
    default = FALSE,
    help = "When used with `--initial-setup` stream the tweets."
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
flog.info(
  "Starting twitter stream with arguments:
\tdatabase name: %s
\traw data dir: %s
\tsearch query: %s
\tstream tweets: %s
\tstream timeout: %s
\tstream chunks: %s",
  opt$database, opt$raw_data_dir, opt$keys,
  opt$initial_setup == opt$force_stream, 
  opt$stream_timeout, opt$stream_chunks
)
if (opt$initial_setup) {
  setup_database(opt$database)
}

if (opt$initial_setup == opt$force_stream) {
  flog.info("Creating the twitter authorization token")
  token <- rtweet::create_token(
    app = 'app',
    consumer_key = Sys.getenv('TW_CONSUMER_KEY'),
    consumer_secret = Sys.getenv('TW_CONSUMER_SECRET'),
    access_token = Sys.getenv('TW_ACCESS_TOKEN'),
    access_secret = Sys.getenv('TW_ACCESS_SECRET')
  )

  for (i in 1:opt$stream_chunks) {
    flog.info("Starting stream chunk number %s", i)
    tweet_file <- get_tweets(
      opt$keys,
      timeout = opt$stream_timeout,
      raw_data_dir = opt$raw_data_dir
    )
    tweets <- transform_tweets(tweet_file)
    load_tweets(tweets, database = opt$database)
    flog.info("Chunk %s completed", i)
  }

  flog.info("Stream completed")
}

