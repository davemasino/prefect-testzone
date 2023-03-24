import requests
import pandas as pd
from prefect import flow, task
 
@task
def hackernews_top_story_ids():
    top_story_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()
    return top_story_ids[:10]

@task
def hackernews_top_stories(ids):
    results = []
    for item_id in ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)

    df = pd.DataFrame(results)
    return df

@flow
def hackernews_flow():
    ids = hackernews_top_story_ids()
    df = hackernews_top_stories(ids)
    print(df)

hackernews_flow()
