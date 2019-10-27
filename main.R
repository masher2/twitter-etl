# Main ETL file
library(DBI)
library(rtweet)


#' Set up the database
#'
#' Given a name contruct the database to hold the tweets data.
setup_database <- function(db_name = "tweets.db") {
  conn <- DBI::dbConnect(RSQLite::SQLite(), db_name)
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
get_tweets <- function(keys, timeout=600) {
  query <- paste(keys, collapse=",")
  filename <- paste0("stream_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json")
  rtweet::stream_tweets(
    q = query,
    timeout = timeout,
    parse = FALSE,
    file_name = filename
  )

  filename
}


#' Process the tweets into a dataframe
transform_tweets <- function(filename) {
  df <- rtweet::parse_stream(filename)
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
load_tweets <- function(tweets) {
}


# Main loop ----
main <- function() {
  token <- create_token(
    app = 'app',
    consumer_key = Sys.getenv('TW_CONSUMER_KEY'),
    consumer_secret = Sys.getenv('TW_CONSUMER_SECRET'),
    access_token = Sys.getenv('TW_ACCESS_TOKEN'),
    access_secret = Sys.getenv('TW_ACCESS_SECRET')
  )
  keys <- c('#chile', '#chiledesperto', '#santiago')

  counter <- 0

  while(counter <= 6) {
    tweet_file <- get_tweets(keys)
    counter <- counter + 1
  }
}
