import requests
import pandas as pd
from prefect import flow, task
 
@task
def hackernews_top_story_ids(url):
    top_story_ids = requests.get(
        url
    ).json()
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
def hn_pipeline():
    url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    ids = hackernews_top_story_ids(url)
    df = hackernews_top_stories(ids)
    
print(hn_pipeline())
