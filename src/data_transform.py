import pandas as pd


def cleaning(df1, df2):
    """"""

    COL = "date"
    df1[COL] = pd.to_datetime(df1[COL])
    df2[COL] = pd.to_datetime(df2[COL])
    return df1, df2


def filter_join(df1, df2):
    """"""

    df2 = df2[df2["country"] == "US"]
    df2.drop(["country"], axis=1, inplace=True)
    return pd.merge(df1, df2, how="inner", on=["date"])


def transformation(nyt_ds, jh_ds):
    """"""

    # Remove and rename columns
    jh_ds.drop(["Province/State", "Confirmed", "Deaths"], axis=1, inplace=True)
    jh_ds.rename(
        {
            "Date": "date",
            "Country/Region": "country",
            "Recovered": "recovered",
        },
        axis=1,
        inplace=True,
    )

    nyt_ds, jh_ds = cleaning(nyt_ds, jh_ds)
    merged_data = filter_join(nyt_ds, jh_ds)

    merged_data.sort_values(by=["date"], ascending=True, inplace=True)

    return merged_data


if __name__ == "__main__":
    n = pd.read_csv("sample_data/nyt_data.csv")
    j = pd.read_csv("sample_data/jh_data.csv")

    m = transformation(n, j)
    print(m.info(), m.head())
