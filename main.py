from src import data_fetch
from src import data_transform
from src import data_load

def main():
    NYT_DATASET = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
    JH_DATASET = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'

    nyt_data = data_fetch.fetch_data(NYT_DATASET)
    jh_data = data_fetch.fetch_data(JH_DATASET)



if __name__ == "__main__":
    main()