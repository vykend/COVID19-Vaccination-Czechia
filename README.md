# COVID19 Vaccinations in Czechia

I find data provided by the Czech [Ministry of Health](https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19) (section "COVID-19: Přehled vykázaných očkování podle očkovacích míst ČR") to be quite impractical to use in  Google Spreadsheets and such, mainly due to their size (although they are very detailed).
So I modified [OWID data parser](https://github.com/owid/covid-19-data/blob/master/scripts/scripts/vaccinations/automations/batch/czechia.py) to include more info in multiple files an have it run every morning (after oficial update of the data) by the Github Actions environment.

## Requirements
- python3
- pandas

## Run
`$ python3 vaccinations_czechia.py`

## Generated files
Numbers are cumulative since the start of the vaccination.

### aggregation.csv
Summary of which vaccines were used, how many 1st (`people_vaccinated`) and 2nd doses (`people_fully_vaccinated`) and their sum in time.

### by_manufacturer.csv
Summary of how many doses of which vaccine were used in time.

### by_region.csv
Summary of how many people in regions got a shot (to today's date).

### by_date_and_region.csv
Summary of doses adminitered in regions in time.
