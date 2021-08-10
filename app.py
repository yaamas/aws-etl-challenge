def extract():
    import pandas as pd
    import requests as req

    NYT_DATASET = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
    JH_DATASET = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'

    def fetch_data(url, filename):
        res = req.get(url)

        if res.status_code != 200:
            res.raise_for_status()
        
        with open(filename, 'wb') as file:
            file.write(res.content)
            

    nyt_ds = fetch_data(NYT_DATASET, 'nyt_dataset.csv')
    jh_ds = fetch_data(JH_DATASET, 'jh_dataset.csv')

def transform():
    def merge_data():
        pass


def load():
    def init_db():
        pass

    def check_database_exists():
        pass

    def insert_data():
        pass



"""
1. first call
2. check db exists
3. if exists fetch data, take today's data
4. fetch jh data
5. transform both data
6. Load transformed data into db

"""