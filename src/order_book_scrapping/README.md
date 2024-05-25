## Order Book Scrapping

### Partitions Description

The output of the Order Book Scrapping sits under the ```outputs/fake_s3_bucket/de_technical_challenge/scrapping``` path
simulating a S3 bucket, with a bucket sufix of the repository and the generic name to identify the data contained 
in the said sufix.

Under this path the partitions of the outputs are partitioned in the following way:

```
|.../bucket_prefix
|    |---book_name                                      (This will allow us to filter early by book_name)
|           |---year
|               |--- month
|                   |--- day
|                       |--- hour
|                           |--- 10 minutes bucket      (This will allow us to filter by the created_at book_order without opening files without need)
```

#### Explanation

The information being requested, being computed per second and storing batches of 10 minutes worth of API calls
are meant to enable the Markets team to monitor, analyze and create specific custom alerts in a second observation.

Having a file that stores at most a record per second in 10 minutes intervals can guide us to understand filters on the 
spread will be by book_name and latest's book_order spread values.

The partition selected by the book_order created_at truncated in a 10 minutes window allows us to not have "useless"
partitions of minutes or seconds (when the data extraction required won't have a physicall improvement to filter by that
granularity). But it's important to mention why not to stop at hour partition; the 10 minutes bucket partition is
selected because filters at second granularity will require to scan partitions of 6 files of at most 600 records, 
which are "only" 3600 records, but the I/O of scanning all 6 files when we know the second, and thus the minute detail
can allow us to jump directly into the 10 minutes bucket file, which can be previously identified by the partitioned location
in a 10 minutes bucket.