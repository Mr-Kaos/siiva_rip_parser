# SiIva Rip Parser

This is a basic tool that is used to get data about rips for the RipDB by scraping data from the SiIvaGunner Wiki (and other rip-related wikis).

**This tool does not intend to undermine the work of the individuals who have worked on the pages being scraped, and only grabs the jokes, rippers and other rip-specific metadata.** All other content on the pages are ignored by this tool.

*This was a quickly built, rudimentary tool to get rip data, so there isn't a whole lot of documentation, sorry about that.*

## Requirements

For this too to work, you will need the following:

- A downloaded copy of the [SiIvaGunner Rips spreadheet](https://docs.google.com/spreadsheets/d/1B7b9jEaWiqZI8Z8CzvFN1cBvLVYwjb5xzhWtrgs4anI) in `.xlxs` format.  
  *This spreadsheet should be placed in the root of the directory, named "`SiIvaGunnerRips.xlsx`".*
- The `openpyxl` python library
- Optional (if you don't want to scrape the fandom yourself, which takes hours), an archive of cached fandom pages.  
  This can be downloaded from [this  Dropbox link](https://www.dropbox.com/scl/fi/b32qf1vjg7yxpiuy2pucl/siiva_parser_fandom_cache.zip?rlkey=0hgup979d0ziwu3n8yi15q7mh&st=4h4o481p&dl=1). Once downloaded, place it in a folder named "`cache`" in the root of this repo.
  *(Note that this archive is a bit outdated from June 2025.)*

## Running

If the spreadsheet file is in the same directory as `parser.py`, then you can simply run the parser using `python3 parser.py`.

Also make sure the `meta_jokes.csv` file exists in the root as well. This is essentially a dictionary the parser uses to find jokes in the fandom pages.
