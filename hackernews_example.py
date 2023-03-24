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

# @task
# def parse_fact(response):
#     fact = response["fact"]
#     print(fact)
#     return fact

# @flow
# def api_flow(url):
#     fact_json = call_api(url)
#     fact_text = parse_fact(fact_json)
#     return fact_text

# api_flow("https://catfact.ninja/fact")


###
# import pandas as pd
# import requests

# from dagster import MetadataValue, Output, asset


# @asset
# def hackernews_top_story_ids():
#     """
#     Get top stories from the HackerNews top stories endpoint.
#     API Docs: https://github.com/HackerNews/API#new-top-and-best-stories
#     """
#     top_story_ids = requests.get(
#         "https://hacker-news.firebaseio.com/v0/topstories.json"
#     ).json()
#     return top_story_ids[:10]


# # asset dependencies can be inferred from parameter names
# @asset
# def hackernews_top_stories(hackernews_top_story_ids):
#     """Get items based on story ids from the HackerNews items endpoint"""
#     results = []
#     for item_id in hackernews_top_story_ids:
#         item = requests.get(
#             f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
#         ).json()
#         results.append(item)

#     df = pd.DataFrame(results)

#     # recorded metadata can be customized
#     metadata = {
#         "num_records": len(df),
#         "preview": MetadataValue.md(df[["title", "by", "url"]].to_markdown()),
#     }

#     return Output(value=df, metadata=metadata)