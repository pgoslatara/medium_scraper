from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from functools import lru_cache
import json
import logging
import os
from pathlib import Path
import requests
from typing import Dict, List
import uuid
from utils.utils import *


class MediumWebScraper:
    def __init__(self, lookback_days: int, tags: List[str]):
        self.base_url = "https://medium.com/tag"
        self.lookback_days = lookback_days
        self.tags = tags

    @lru_cache
    def get_extracted_at(self) -> datetime:
        return datetime.utcnow()

    @lru_cache
    def get_extracted_at_epoch(self) -> int:
        return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())

    @lru_cache
    def get_extraction_id(self) -> str:
        return str(uuid.uuid4())

    def run(self):
        for tag in self.tags:
            logging.info(f"Scraping blogs with tag '{tag}...")

            start_date = (
                datetime.utcnow() - timedelta(days=1 + self.lookback_days)
            ).date()
            end_date = (datetime.utcnow() - timedelta(days=(1))).date()
            dates_array = [
                start_date + timedelta(days=x)
                for x in range((end_date - start_date).days)
            ]
            logging.info(f"Generated {len(dates_array)} dates...")

            scraped_data = []
            for date in dates_array:
                scraped_data += self.scrape_blogs(date, tag)

            self.store_blogs(
                [dict(t) for t in {tuple(d.items()) for d in scraped_data}], tag
            )

        return self.get_extraction_id()

    def scrape_blogs(self, date_of_interest: datetime, tag: str) -> List[dict]:
        url = f"{self.base_url}/{tag}/archive/{date_of_interest.year}/{date_of_interest.month:02}/{date_of_interest.day:02}"
        logging.info(
            f"Scraping blogs on {date_of_interest.strftime('%Y=%m-%d')}: {url=}..."
        )

        page = requests.get(url)
        soup = BeautifulSoup(page.text, "html.parser")

        if (
            page.text.find(
                "col u-inlineBlock u-width265 u-verticalAlignTop u-lineHeight35 u-paddingRight0"
            )
            == -1
        ):
            logging.info("Monthly summary displayed, results require deduplication.")
            url = url[: url.rfind("/")]

        if (
            page.text.find("These were the top 10 stories tagged with ") > 0
        ):  # i.e. No matching blogs on this date
            logging.info("No blogs published on this date.")
            return []
        else:
            stories = soup.find_all(
                "div", class_="streamItem streamItem--postPreview js-streamItem"
            )
            logging.info(f"Found {len(stories)} stories...")

            base_data = {
                "extraction_id": self.get_extraction_id(),
                "extracted_at": self.get_extracted_at(),
                "extracted_at_epoch": self.get_extracted_at_epoch(),
                "extraction_url": url,
                "tag": tag,
            }

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
                        author_box.find("span", class_="readingTime")["title"].split()[
                            0
                        ]
                    )
                except:
                    data["reading_time_minutes"] = -1

                data["published_at"] = story.find("time").get("datetime")
                data["title"] = story.find("h3").text if story.find("h3") else "-"
                data["subtitle"] = story.find("h4").text if story.find("h4") else "-"

                try:
                    data["story_url"] = story.find(
                        "a",
                        class_="button button--smaller button--chromeless u-baseColor--buttonNormal",
                    )["href"].split("?source=tag_archive")[0]
                except:
                    try:
                        title_index = str(story).find(data["title"])
                        href_index = str(story)[title_index:].find("data-action-value")
                        href_base = str(story)[title_index + href_index + 19 :]
                        data["story_url"] = href_base[: href_base.find('"')]
                    except:
                        data["story_url"] = "-"

                sorted_data = dict(sorted(data.items(), key=lambda x: (x[0])))
                web_data.append(sorted_data)

            return web_data

    def store_blogs(self, blogs: str, tag: str):
        file_name = f"{get_output_dir()}/tag={tag}/extracted_at={self.get_extracted_at_epoch()}/extraction_id={self.get_extraction_id()}.json"
        logging.info(f"Saving {len(blogs)} blogs to {file_name}...")
        Path(file_name[: file_name.rfind("/")]).mkdir(parents=True, exist_ok=True)
        with open(file_name, "w", encoding="utf-8") as f_write:
            json.dump(
                blogs,
                f_write,
                ensure_ascii=False,
                indent=4,
                default=str,
            )
