import sys
import time
from datetime import datetime
from utils import OrderBook


def main(order_book_name):
    order_book_name = order_book_name
    records = []
    bucket_interval = (datetime.now().minute // 10) * 10
    order_book = OrderBook()

    while True:
        # Get Order Book
        record, order_book_date = order_book.get_book_order_spread_record(book_name=order_book_name)
        records.append(record)

        # Check if it's time to save based on the current record's timestamp
        record_minute_bucket = (order_book_date.minute // 10) * 10

        # Save records at the end of each 10-minute bucket
        if record_minute_bucket > bucket_interval:
            order_book.save_records(payload=records, order_book_name=order_book_name)
            records = [record]  # Clear the records list after saving
            bucket_interval = record_minute_bucket  # Increase 10 minutes bucket interval

        # Wait one Second between calls
        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv[1:])
