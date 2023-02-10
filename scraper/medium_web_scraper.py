from bs4 import BeautifulSoup
import json
import requests
from typing import Dict, List

BASE_URL = "https://medium.com/tag/"
# TAGS = ["apache-airflow", "dbt"]


class MediumWebScraper:
    def scrape_blogs(self) -> List[Dict]:
        url = BASE_URL + "apache-airflow" + "/archive/2022/12/27"

        page = requests.get(url)
        soup = BeautifulSoup(page.text, "html.parser")

        stories = soup.find_all(
            "div", class_="streamItem streamItem--postPreview js-streamItem"
        )
        print(f"Found {len(stories)} stories...")

        web_data = []
        for story in stories:
            data = {}

            author_box = story.find(
                "div", class_="postMetaInline u-floatLeft u-sm-maxWidthFullWidth"
            )
            data["author_url"] = author_box.find("a")["href"]
            data["author_name"] = author_box.find(
                "a",
                class_="ds-link ds-link--styleSubtle link link--darken link--accent u-accentColor--textNormal u-accentColor--textDarken",
            ).text

            try:
                data["reading_time_minutes"] = int(
                    author_box.find("span", class_="readingTime")["title"].split()[0]
                )
            except:
                data["reading_time_minutes"] = -1

            data["published_at"] = story.find("time").get("datetime")
            data["title"] = story.find("h3").text if story.find("h3") else "-"
            data["subtitle"] = story.find("h4").text if story.find("h4") else "-"
            data["story_url"] = story.find(
                "a",
                class_="button button--smaller button--chromeless u-baseColor--buttonNormal",
            )["href"].split("?source=tag_archive")[0]

            web_data.append(data)

        return web_data

    def store_blogs(self, blogs: str, path: str):
        with open(path, "w", encoding="utf-8") as f_write:
            json.dump(
                blogs,
                f_write,
                ensure_ascii=False,
                indent=4,
                default=str,
            )
