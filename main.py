import logging

from src import data_fetch, data_transform

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    NYT_DATASET = (
        "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    )
    JH_DATASET = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"

    nyt_data = data_fetch.fetch_data(NYT_DATASET)
    jh_data = data_fetch.fetch_data(JH_DATASET)

    merged_data = data_transform.transformation(nyt_data, jh_data)

    return merged_data


if __name__ == "__main__":
    main()
