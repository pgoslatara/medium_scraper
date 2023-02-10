import json
from typing import Dict, List
from models.source.pokeapi import Pokeapi
from models.target.pokemon import Pokemon


class PokemonParser:
    pokemon: List[Pokemon] = []

    def parse_pokemon(self, pokeapi_json: Dict):
        pokeapi = Pokeapi(**pokeapi_json)
        try:
            for result in pokeapi.results:
                pokemon = Pokemon(
                    name=result.name,
                    url = result.url
                )
                self.pokemon.append(pokemon)
        except AttributeError as e:
            print(e)


    def store_pokemon(self, path: str):
        with open(path, "w", encoding="utf-8") as f_write:
            json.dump(
                [model.dict() for model in self.pokemon],
                f_write,
                ensure_ascii=False,
                indent=4,
                default=str,
            )
