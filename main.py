from scraper.medium_web_scraper import MediumWebScraper


if __name__ == "__main__":
    scraper = MediumWebScraper()
    medium_web_json = scraper.scrape_blogs()
    scraper.store_blogs(medium_web_json, "output/medium_blogs.json")
