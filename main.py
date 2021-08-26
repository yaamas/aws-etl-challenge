import logging

from src import data_fetch, data_load, data_transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    nyt_data, jh_data = data_fetch.fetch_data()

    merged_data = data_transform.transform_data(nyt_data, jh_data)

    data_load.load(merged_data)


if __name__ == "__main__":
    main()
