# medium_scraper

[Latest files](https://flatgithub.com/<repo>/blob/<branch>/output/result.json?filename=<path>)

## Setup

1. Run `python -m venv .venv` to create a virtual environment in the subfolder `.venv`.
2. Activate the virtual environment using `source .venv/bin/activate` on Mac, and `.venv\Scripts\acitvate.bat` on Windows.
3. Run `pip install -r requirements.txt` and `pip install -r requirements_local.txt` to install requirements.
4. Run `python main.py` to run the scraper.
5. There should be a file in `output/pokemon.json` with the scraped results.
6. Create a repository on Github and push the code to the repo. This should automatically start an Action and result in a commit with the updated `output/pokemon.json` file in the repo. See the `.github/workflows/action.yml` for the steps that are ran.
7. Go to the url of the `pokemon.json` file and prepend `flat` to `github.com` e.g. `https://flatgithub.com/<owner>/<repository>/blob/master/output/pokemon.json` -> `https://.com/<owner>/<repository>/blob/master/output/pokemon.json` to view the results in Flatviewer.

## Next steps

1. Remove the `pokeapi` scraper and Models.
2. Find another API you want to scrape, and fetch the `json` results by calling an endpoint in the API. Store them locally in a file e.g. `results.json`.
3. Run `make codegen path=results.json name=<model_name>` to create the model in `models/source`. On Windows: Either install `make` on Windows or just copy the commands from the `makefile` and run them manually.
4. Manually trim the generated `source` Model for the attributes that you need, perhaps rename some Models for clarity.
5. Repeat steps 4 & 5 for all the endpoints you want to scrape.
6. By hand, create a `target` model which contains the data you want to store.
7. Add a scraper in the `scraper` folder that retrieves the `json` results using `requests`.
8. Create a parser function that takes the `json` results, parses them using the `Pydantic` model you just generated and then converts the `source` Model into a `target` Model.
9. Store the `target` Model(s) as `json`/`csv` into the repository.
10. Update the `cron` schedule in `.github/workflows/action.yml` to suit your needs.
11. Push the changes to the upstream repository and watch the `Action` run your scraper on a schedule!
# medium_scraper
