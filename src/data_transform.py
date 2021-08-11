import pandas as pd


def cleaning(df, col):
    df[col] = pd.to_datetime(df[col])
    return df


def filtering(nyt_ds, jh_ds):
    jh_ds = jh_ds[jh_ds["Country/Region"] == "US"]
    jh_ds.reset_index(drop=True, inplace=True)

    jh_dates = set(jh_ds["Date"])
    nyt_dates = set(nyt_ds["date"])
    intersection = jh_dates.intersection(nyt_dates)

    print(len(intersection))


def joining(dates):
    # .sort_values(by='dates', ascending=True)
    merged_df = pd.DataFrame(
        {"dates": dates, "cases": [], "deaths": [], "recoveries": []}
    )
    for i in range(len(dates)):
        merged_df.at[
            i,
        ]


def transformation(nyt_ds, jh_ds):
    nyt_ds = cleaning(nyt_ds, "date")
    jh_ds = cleaning(jh_ds, "Date")
    nyt_ds, jh_ds = filtering(nyt_ds, jh_ds)


if __name__ == "__main__":
    n = pd.read_csv("sample_data/nyt_data.csv")
    j = pd.read_csv("sample_data/jh_data.csv")

    transformation(n, j)

    # print(j.info())

    # print(j.info())
    # print(n.info())
