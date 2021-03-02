"""
Original: https://github.com/owid/covid-19-data/blob/master/scripts/scripts/vaccinations/automations/batch/czechia.py

Since we need to translate vaccine names, we'll check that no new
manufacturers were added, so that we can maintain control over this

IMPORTANT: If a new vaccine is added, see if it requires a single dose
or two doses. If it's a single-dose one, make sure to fix the calculation
of `total_vaccinations`
"""


import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, parse_dates=["datum"])

#datum,ockovaci_misto_id,ockovaci_misto_nazev,kraj_nuts_kod,kraj_nazev,volna_kapacita,maximalni_kapacita,kalendar_ockovani
def check_columns(input: pd.DataFrame) -> pd.DataFrame:
    expected = [
        "datum",
        "ockovaci_misto_id",
        "ockovaci_misto_nazev",
        "kraj_nuts_kod",
        "kraj_nazev",
        "volna_kapacita",
        "maximalni_kapacita",
        "kalendar_ockovani",
    ]
    if list(input.columns) != expected:
        raise ValueError(
            "Wrong columns. Was expecting {} and got {}".format(
                expected, list(input.columns)
            )
        )
    return input


def base_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    
    return (
        input.pipe(check_columns)
    )




def main():
    source = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-registrace.csv"

    by_date_output = "registration_by_date.csv"
    by_region_output = "registrations_by_region.csv"
    by_date_and_region_output = "registrations_by_date_and_region.csv"

    base = read(source).pipe(base_pipeline)

    base.pipe(breakdown_per_date).to_csv(by_date_output, index=False)
    base.pipe(breakdown_per_region).to_csv(by_region_output, index=False)
    base.pipe(breakdown_per_date_and_region).to_csv(by_date_and_region_output)


if __name__ == "__main__":
    main()
