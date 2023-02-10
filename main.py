from parser.pokemon_parser import PokemonParser
from scraper.pokeapi_scraper import PokeAPIScraper


if __name__ == "__main__":
    scraper = PokeAPIScraper()
    pokeapi_json = scraper.scrape_pokemon()
    parser = PokemonParser()
    parser.parse_pokemon(pokeapi_json)
    parser.store_pokemon("output/pokemon.json")
