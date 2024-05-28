# overcast-data

[Overcast](https://overcast.fm) podcast personal data scraper.

## Setup

Design to run via GitHub Actions. First, Fork this repository but only the `main` branch. Data is stored on `gh-pages` and you don't want to store with my personal data.

Then set up GitHub Action Repository secrets for the following:

* `ENCRYPTION_KEY`: Any random 64 byte base64 encoded string
* `OVERCAST_COOKIE`: The `o=` value from your [overcast.fm](https://overcast.fm/) web session
