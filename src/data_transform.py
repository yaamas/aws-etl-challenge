import logging

import pandas as pd

logger = logging.getLogger(__name__)


def cleaning(df1, df2):
    """"""

    # Remove and rename columns
    df2.drop(["Province/State", "Confirmed", "Deaths"], axis=1, inplace=True)

    logger.debug(
        "Dropped 'State', 'Confirmed', 'Deaths' columns from JH Dataset"
    )

    df2.rename(
        {
            "Date": "date",
            "Country/Region": "country",
            "Recovered": "recovered",
        },
        axis=1,
        inplace=True,
    )

    # CHeck for NAN, NA values
    # print([1 if i is True else 0 for i in df2['recovered'].isna()])

    # Drop NA/INF values from recovered data
    df2["recovered"].fillna(value=0, inplace=True)
    df2["recovered"] = df2["recovered"].astype("int64")

    COL = "date"
    df1[COL] = pd.to_datetime(df1[COL])
    df2[COL] = pd.to_datetime(df2[COL])

    # df1['cases'] = pd.to

    logger.debug(
        f"Converted {len(df1)} and {len(df2)} into Pandas.Datetime objects"
    )

    return df1, df2


def filter_join(df1, df2):
    """"""
    # TODO: Fix this.
    # The place where chaining is taking place.
    # Refer: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
    df2 = df2[df2["country"] == "US"]

    logger.debug("Filterd JH Dataset for US Entries")

    df2.drop(["country"], axis=1, inplace=True)

    logger.debug("Dropped country column from JH Dataset")

    merged = pd.merge(df1, df2, how="inner", on=["date"])

    logger.debug("Merged two dataframes on date, inner join")

    return merged


def transform_data(nyt_ds, jh_ds):
    """"""
    nyt_ds, jh_ds = cleaning(nyt_ds, jh_ds)
    merged_data = filter_join(nyt_ds, jh_ds)

    merged_data.sort_values(by=["date"], ascending=True, inplace=True)

    return merged_data


if __name__ == "__main__":
    n = pd.read_csv("sample_data/nyt_data.csv")
    j = pd.read_csv("sample_data/jh_data.csv")

    m = transform_data(n, j)
    print(m.info(), m.head())
