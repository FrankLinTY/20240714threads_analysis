import pandas as pd
import requests
from datetime import datetime, timedelta


USER_ID = 7130039317097562

ACCESS_TOKEN = "THQWJXekN0Y3p3QnJ3ZAzdfZA3pCX18tWWg5a3ViQk96TlpwUGV2QmJqR1V2NFlNd2ZAwa2hIMUJlQWJqYWhjdUhZAYUV1SDhkaDZAWWmZAvOHVKaWVkY1V2RmNJbzNiWEQwWUZAuSUJ6UHdZAS3hiOF9IQTZAsMGJxUWVvcXl0dGhRVmpEU2FLbno3TlliSHNiVkhmbE42TFEZD"

# set up api vars
API_URL = "https://graph.threads.net/v1.0"
INSIGHT_METRIC_LIST = ["views", "likes", "replies", "reposts", "quotes"]
LIMIT = 5  # limit for get threads

# set up datetimes
BACKFILL_DATE_INTERVAL = 7

def get_threads_df(
    user_id: str,
) -> pd.DataFrame:

    # set up params
    start_date_dt = datetime.now() - timedelta(days=BACKFILL_DATE_INTERVAL)
    limit = LIMIT if LIMIT else 100
    params = {
        "fields": "id,permalink,username,timestamp,text",
        "since": str(start_date_dt.isoformat()),
        "access_token": ACCESS_TOKEN,
        "limit": limit,
    }

    resp = requests.get(f"{API_URL}/{user_id}/threads", params=params)

    if resp.status_code != 200:
        raise Exception(resp.json())

    df = pd.DataFrame.from_dict(resp.json().get("data", []))

    return df


def get_thread_id_insights_by_id(
    thread_id: str,
) -> dict:
    
    print(f"insights for thread: {thread_id}...")

    # set up params
    params = {
        "metric": ",".join(INSIGHT_METRIC_LIST),
        "access_token": ACCESS_TOKEN,
    }

    resp = requests.get(f"{API_URL}/{thread_id}/insights", params=params)

    if resp.status_code != 200:
        raise Exception(resp.json())

    # re-gen data metrics

    metric_dict = {}

    for data in resp.json().get("data", []):

        metric_dict[data.get("name")] = data.get("values")[0].get("value")

        continue

    return metric_dict


def main() -> pd.DataFrame:

    # get threads
    print("getting threads...")
    df_threads = get_threads_df(user_id=USER_ID)

    # get insights
    print("getting insights...")
    df_threads["insights"] = df_threads["id"].apply(get_thread_id_insights_by_id)

    # create columns for insights
    for metric in INSIGHT_METRIC_LIST:
        df_threads[metric] = df_threads["insights"].apply(lambda dict: dict.get(metric))
        continue

    # drop insights column
    df_threads.drop(columns=["insights"], inplace=True)

    return df_threads


if __name__ == "__main__":
    df = main()
    print(df)