# ETL Process for Twitter Academic API - Full Archive Endpoint

This projects implements an ETL process streaming data from Twitter's Academic API v2 (Full Archive Endpoint).

The [TweetStreamer](TweetCollector_FullArchiveAPI/TweetStreamer.py) class handles tasks relating to tweet streaming, including authentication to the API and building the query. It also uses the [TweetLoader](TweetCollector_FullArchiveAPI/TweetLoader.py) to transform the data to abide the schema and load the Tweets to a PostgreSQL instance.

Note that if the parameter "recreate_db" to the TweetSreamer is False, you must have an existing table in PostgreSQL defined in [Tables](TweetCollector_FullArchiveAPI/Tables.py). This option is suitable after the first batch of collections, as you will be adding records to an existing table. If True, you will create a table that matches the schema defined in Tables.

## Sources

1. [Twitter Sample Code for Authentication](https://github.com/twitterdev/Twitter-API-v2-sample-code/tree/main/Full-Archive-Search)

2. [Twitter Documentation for Pagination](https://developer.twitter.com/en/docs/twitter-api/pagination)

3. [Twitter Documentation on Filtering Tweets By Location](https://developer.twitter.com/en/docs/tutorials/filtering-tweets-by-location)

4. [SQLAlchemy Documentation](https://docs.sqlalchemy.org/en/13/orm/session_basics.html)
