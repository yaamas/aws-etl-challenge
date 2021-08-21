import io

import pandas as pd
import requests as req


def fetch_data(url):
    url_data = req.get(url)

    if url_data.status_code != 200:
        url_data.raise_for_status()

    raw_data = (url_data.content).decode("utf-8")
    df = pd.read_csv(io.StringIO(raw_data))

    return df


if __name__ == "__main__":
    from data_transform import transformation

    NYT_DATASET = (
        "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    )
    JH_DATASET = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"

    n = fetch_data(NYT_DATASET)
    j = fetch_data(JH_DATASET)

    merg_data = transformation(n, j)
    print(merg_data.head(), merg_data.info())
