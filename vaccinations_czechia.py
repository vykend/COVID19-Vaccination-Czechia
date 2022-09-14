"""
Original: https://github.com/owid/covid-19-data/blob/master/scripts/scripts/vaccinations/automations/batch/czechia.py

Since we need to translate vaccine names, we'll check that no new
manufacturers were added, so that we can maintain control over this

IMPORTANT: If a new vaccine is added, see if it requires a single dose
or two doses. If it's a single-dose one, make sure to fix the calculation
of `total_vaccinations`
"""


import pandas as pd

def enrich_total_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        total_vaccinations=input.people_vaccinated + input.people_fully_vaccinated
    )


vaccine_mapping = {
    "Comirnaty": "Pfizer/BioNTech",
    "Spikevax": "Moderna",
    "SPIKEVAX": "Moderna",
    "VAXZEVRIA": "Oxford/AstraZeneca",
    "COVID-19 Vaccine Janssen": "Johnson&Johnson",
    "Comirnaty 5-11": "Pfizer/BioNTech Kids",
    "Comirnaty Original/Omicron BA.1": "Pfizer/BioNTech original + BA.1",
    "Spikevax bivalent Original/Omicron BA.1": "Moderna bivalent BA.1",
    "Comirnaty Original/Omicron BA.4/BA.5": "Pfizer/BioNTech original + BA.4/5",
    "Nuvaxovid": "Novavax",
    "Sinovac": "Čína",
    "Sinopharm": "Čína",
    "Covishield": "Indie/AstraZeneca",
    "COVAXIN": "Indie/Bahrat Biotech"
}

one_dose_vaccines = ["Johnson&Johnson"]

def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source, parse_dates=["datum"])


def check_columns(input: pd.DataFrame) -> pd.DataFrame:
    expected = ['id', 'datum', 'vakcina', 'vakcina_kod', 'poradi_davky', 'kraj_nazev', 'kraj_nuts_kod', 'orp_bydliste', 'orp_bydliste_kod', 'pocet_davek']
    if list(input.columns) != expected:
        raise ValueError(
            "Wrong columns. Was expecting {} and got {}".format(
                expected, list(input.columns)
            )
        )
    return input


def check_vaccine_names(input: pd.DataFrame) -> pd.DataFrame:
    input = input.dropna(subset=["vakcina"])
    unknown_vaccines = set(input.vakcina.unique()).difference(
        set(vaccine_mapping.keys())
    )
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
    return input


def translate_vaccine_names(input: pd.DataFrame) -> pd.DataFrame:
    return input.replace(vaccine_mapping)


def base_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(check_columns)
        .pipe(check_vaccine_names)
        .pipe(translate_vaccine_names)
    )


def breakdown_per_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by=["datum", "vakcina"], as_index=False)["pocet_davek"]
        .sum()
        .sort_values("datum")
        .assign(
            size=lambda df: df.groupby(by=["vakcina"], as_index=False)["pocet_davek"].cumsum()
        )
        .rename(
            columns={
                "datum": "date",
                "vakcina": "vaccine",
                "size": "total_vaccinations",
            }
        )
    )
    
    
def breakdown_per_date_and_region(input: pd.DataFrame) -> pd.DataFrame:
    temp = input.groupby(["datum","kraj_nazev"]).size().unstack(fill_value=0)
    float_col = temp.select_dtypes(include=['float64'])
    for col in float_col.columns.values:
        temp[col] = temp[col].astype('int64')
    return temp

"""
def breakdown_per_date_and_region(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by=["datum", "kraj_nazev"], as_index=False)
        .size()
        .sort_values("datum")
        .assign(
            size=lambda df: df.groupby(by=["kraj_nazev"], as_index=False)["size"].cumsum()
        )
        .rename(
            columns={
                "datum": "date",
                "kraj_nazev": "region",
                "size": "total_vaccinations",
            }
        )
    )
"""

def breakdown_per_region(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by=["kraj_nazev"], as_index=False)["pocet_davek"].sum()
        .sort_values("kraj_nazev")
        .assign(
            size=lambda df: df.groupby(by=["kraj_nazev"], as_index=False)["pocet_davek"].cumsum()
        )
        .rename(
            columns={
                "kraj_nazev": "region",
                "size": "total_vaccinations",
            }
        )
    )


def aggregate_by_date_vaccine(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by=["datum", "vakcina", "poradi_davky"])["pocet_davek"].sum()
        .unstack()
        .reset_index()
    )


def aggregate_by_date(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.groupby(by="datum")
        .agg(
            vaccine=("vakcina", lambda x: ", ".join(sorted(set(x)))),
            people_vaccinated=(1, "sum"),  # 1 means 1st dose
            people_fully_vaccinated=(2, "sum"),
            people_boosted_1=(3,"sum"),
            people_boosted_2=(4,"sum"),
        )
        .reset_index()
    )


def check_first_date(input: pd.DataFrame) -> pd.DataFrame:
    first_date = input.date.min()
    expected = "2020-12-27"
    if first_date != expected:
        raise ValueError(
            "Expected the first date to be {}, encountered {}.".format(
                expected, first_date
            )
        )
    return input


def translate_columns(input: pd.DataFrame) -> pd.DataFrame:
    return input.rename(columns={"datum": "date"})


def format_date(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(date=input.date.astype(str).str.slice(0, 10))


def enrich_cumulated_sums(input: pd.DataFrame) -> pd.DataFrame:
    return input.sort_values(by="date").assign(
        **{
            col: input[col].cumsum().astype(int)
            for col in [
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
                "people_boosted_1",
                "people_boosted_2",
            ]
        }
    )

def infer_one_dose_vaccines(input: pd.DataFrame) -> pd.DataFrame:
    input.loc[input.vakcina.isin(one_dose_vaccines), 2] = input[1]
    return input


def infer_total_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    input.loc[input.vakcina.isin(one_dose_vaccines), "total_vaccinations"] = input[1].fillna(0)
    input.loc[-input.vakcina.isin(one_dose_vaccines), "total_vaccinations"] = input[1].fillna(0) + input[2].fillna(0)
    return input


def global_enrichments(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(enrich_total_vaccinations)
        .pipe(enrich_cumulated_sums)
    )


def global_pipeline(input: pd.DataFrame) -> pd.DataFrame:
    return (
        input.pipe(aggregate_by_date_vaccine)
        .pipe(infer_one_dose_vaccines)
        .pipe(infer_total_vaccinations)
        .pipe(aggregate_by_date)
        .pipe(translate_columns)
        .pipe(format_date)
        .pipe(global_enrichments)
        .pipe(check_first_date)
    )


def main():
    source = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-geografie.csv"

    global_output = "aggregation.csv"
    by_manufacturer_output = "by_manufacturer.csv"
    by_date_and_region_output = "by_date_and_region.csv"
    by_region_output = "by_region.csv"

    base = read(source).pipe(base_pipeline)

    base.pipe(breakdown_per_vaccine).to_csv(by_manufacturer_output, index=False)
    base.pipe(breakdown_per_date_and_region).to_csv(by_date_and_region_output)
    base.pipe(breakdown_per_region).to_csv(by_region_output, index=False)
    base.pipe(global_pipeline).to_csv(global_output, index=False)


if __name__ == "__main__":
    main()
