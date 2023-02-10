from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from functools import lru_cache
import json
import logging
import requests
from typing import Dict, List
import uuid

class MediumWebScraper():
    def __init__(self, lookback_days:int, tag:str):
        self.base_url = "https://medium.com/tag"
        self.lookback_days = lookback_days
        self.tag = tag
        
    def get_extracted_at(self) -> datetime:
        return datetime.utcnow()

    @lru_cache
    def get_extraction_id(self) -> uuid:
        return str(uuid.uuid4())

    def run(self):
        logging.info(f"Scraping blogs with tag '{self.tag}...")

        start_date = (datetime.utcnow() - timedelta(days=1+self.lookback_days)).date()
        end_date = (datetime.utcnow() - timedelta(days=(1))).date()
        dates_array = [start_date+timedelta(days=x) for x in range((end_date-start_date).days)]
        logging.info(f"Generated {len(dates_array)} dates...")

        scraped_data = []
        for date in dates_array:
            scraped_data.append(self.scrape_blogs(date))
        
        self.store_blogs(scraped_data, f"output/medium_blogs_{self.tag}_{self.get_extraction_id()}.json")
    
    def scrape_blogs(self, date_of_interest: datetime.date) -> list[dict]:
        logging.info(f"Scraping blogs on {date_of_interest.strftime('%Y=%m-%d')}...")
        url = f"{self.base_url}/{self.tag}/archive/{date_of_interest.year}/{date_of_interest.month:02}/{date_of_interest.day:02}"
        logging.info(f"{url=}")

        page = requests.get(url)
        soup = BeautifulSoup(page.text, "html.parser")

        stories = soup.find_all(
            "div", class_="streamItem streamItem--postPreview js-streamItem"
        )
        logging.info(f"Found {len(stories)} stories...")

        base_data = {"extraction_id": self.get_extraction_id(), "extracted_at": self.get_extracted_at()}

        web_data = []
        for story in stories:
            data = base_data.copy()

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

            logging.info(data)
            web_data.append(data)

        return web_data

    def store_blogs(self, blogs: str, path: str):
        logging.info(f"Saving to {path}...")
        with open(path, "w", encoding="utf-8") as f_write:
            json.dump(
                blogs,
                f_write,
                ensure_ascii=False,
                indent=4,
                default=str,
            )
