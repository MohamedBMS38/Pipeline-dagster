from setuptools import setup, find_packages

setup(
    name="crypto_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-duckdb",
        "pandas",
        "requests",
        "matplotlib",
        "python-dotenv",
    ],
) 