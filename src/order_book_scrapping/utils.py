import os
import json

import pandas as pd
import yaml
import bitso
import pandas
import logging
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def load_config():
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs.yaml')
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
        return config


class OrderBook:
    def __init__(self):
        # Get Env variables
        load_dotenv()
        bitso_key = os.getenv('BITSO_API_KEY')
        bitso_secret = os.getenv('BITSO_API_SECRET')
        # Generate API instance
        self.api = bitso.Api(bitso_key, bitso_secret)
        # Load the config.yaml file
        self.configs = load_config()

    def extract_order_book(self, book_name: str):
        logging.debug(f"Making {book_name} api call...")
        return self.api.order_book(book_name)

    @staticmethod
    def spread_record_format(
            orderbook_timestamp: datetime,
            book: str,
            bid: float,
            ask: float,
            spread: float
    ) -> object:
        return {
            "orderbook_timestamp": orderbook_timestamp.isoformat(),
            "book": str(book),
            "bid": float(bid),
            "ask": float(ask),
            "spread": float(spread)
        }

    def compute_spread_from_order_book(self, book_name: str, order_book):
        try:
            best_bid = max(order_book.bids, key=lambda x: x.price).price
            best_ask = min(order_book.asks, key=lambda x: x.price).price
            spread = ((best_ask - best_bid) * 100) / best_ask
            return self.spread_record_format(
                orderbook_timestamp=order_book.updated_at,
                book=book_name,
                bid=best_bid,
                ask=best_ask,
                spread=spread
            )
         except Exception as e:
            raise Exception(e)


    def get_book_order_spread_record(self, book_name: str):
        order_book = self.extract_order_book(book_name=book_name)
        logging.debug("Parsing order book response...")
        order_book_date = order_book.updated_at
        spread_record = self.compute_spread_from_order_book(book_name=book_name, order_book=order_book)
        logging.info(f"Order book {book_name} extracted")
        return spread_record, order_book_date

    def save_records(self, payload, order_book_name: str):
        if not payload:
            raise Exception("Payload to be saved is empty, validate API extractions.")

        # Generate output directory
        base_dir = os.path.join(
            self.configs["output_prefix"],
            self.configs["bucket_name"],
            self.configs["output_target"],
            "order_book_scrapping",
            order_book_name
        )

        # Use the timestamp of the first record to determine the directory structure
        timestamp = datetime.fromisoformat(payload[0]['orderbook_timestamp'])

        # Calculate 10-minute bucket
        minute_bucket = (timestamp.minute // 10) * 10

        # Create partitioned directory structure
        year_dir = os.path.join(base_dir, str(timestamp.year))
        month_dir = os.path.join(year_dir, str(timestamp.month).zfill(2))
        day_dir = os.path.join(month_dir, str(timestamp.day).zfill(2))
        hour_dir = os.path.join(day_dir, str(timestamp.hour).zfill(2))
        minute_bucket_dir = os.path.join(hour_dir, f"{minute_bucket:02d}")

        # Create directories if they don't exist
        os.makedirs(minute_bucket_dir, exist_ok=True)

        # Define the file name based on the timestamp
        file_time = f"{timestamp.year}{timestamp.month:02d}{timestamp.day:02d}{timestamp.hour:02d}{minute_bucket:02d}"
        file_name = f"{order_book_name}_{file_time}.parquet"
        file_path = os.path.join(minute_bucket_dir, file_name)

        # Generate Dataframe, cast and save as parquet
        try:
            df = pd.DataFrame(payload)
            cast_df = self.cast_payload(dataframe=df)
            cast_df.to_parquet(file_path)
            logging.info(f"Records saved in: {file_path}")
        except Exception as e:
            logger.error("Save file {file_name} FAILED.")

    @staticmethod
    def cast_payload(dataframe: pd.DataFrame)
        try:
            dataframe[['orderbook_timestamp', 'book']] = dataframe[['orderbook_timestamp', 'book']].astype(str)
            dataframe[['bid', 'ask', 'spread']] = dataframe[['bid', 'ask', 'spread']].astype(float)
        except:
            logger.error("Unable to apply casting as expected, schema changed.")
            raise Exception("Casting Failed, possible change on schema.")
        return dataframe