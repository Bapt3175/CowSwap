from cow_swap.apis.api_client import CoinGeckoClient
from cow_swap.apis.dune_fetcher import DuneDataFetcher
from cow_swap.database.db_provider import PostgreSQLProvider
from cow_swap.processor import Processor
from cow_swap.utils import setup_logging, load_config


def main() -> None:
    logger = setup_logging()

    config = load_config(logger)
    provider = PostgreSQLProvider(**config["db_params"])
    dune_client = DuneDataFetcher(config["dune_api"]["api_key"])
    coingecko_client = CoinGeckoClient()

    processor = Processor(
        dune_fetcher=dune_client,
        coingecko_client=coingecko_client,
        pgsql_provider=provider,
        config=config,
        logger=logger,
    )

    processor.process()


if __name__ == "__main__":
    main()
