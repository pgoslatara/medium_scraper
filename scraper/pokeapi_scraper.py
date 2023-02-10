from typing import Dict, List
import requests
import json

BASE_URL = "https://pokeapi.co/api/v2/"

class PokeAPIScraper():

    def scrape_pokemon(self) -> Dict:
        url = BASE_URL + "/pokemon"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:95.0) Gecko/20100101 Firefox/95.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Content-Length": "0",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
        }

        response = requests.request("GET", url, headers=headers)

        return json.loads(response.text)
