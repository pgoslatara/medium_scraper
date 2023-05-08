from setuptools import setup, find_packages

requirements = [
    "beautifulsoup4",
    "black==22.8.0",
    "dbt-duckdb",
    "duckdb",
    "duckcli",
    "mypy",
    "plotly>=5.13.0",
    "pre-commit",
    "pyarrow",
    "requests",
    "sh>=2.0.0",
    "tabulate",
]
setup_requirements = ["pytest-runner"]
tests_requirements = ["pytest>=7.1.1"]
dev_requirements = [
    "wheel==0.37.1",
] + tests_requirements
extras_requirements = {"dev": dev_requirements}

setup(
    include_package_data=True,
    name="medium-scraper",
    packages=find_packages(include=["src"]),
    extras_require=extras_requirements,
    install_requires=requirements,
    setup_requires=setup_requirements,
    tests_require=tests_requirements,
)
