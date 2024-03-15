# demo in golang to use worker pool for mysql
Initially inspired by this medium article ... (was deleted meanwhile, maybe because of the lack of quality)

I tried to improve my go skills and added a few features:

 - better error handling
 - better concurrency control for quitting the workers
 - better sql performance by using batches

Still the code is not very clean, but it works.

Next thing is to provide better documentation or a small blog article.

## docker compose

There is a docker compose file included in the repo to provide a mariadb instance for testing.
It comes together with [adminer](https://www.adminer.org/de/) a minimalistic web admin ui for mysql.

## test data

Test data can be downloaded with
```sh
wget http://downloads.majestic.com/majestic_million.csv
```
