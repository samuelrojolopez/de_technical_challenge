# tests/test_order_book_spread.py
import sys
import os
import pytest
from datetime import datetime
from unittest.mock import Mock

# Add the root directory of your project to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.order_book_scrapping.utils import OrderBook


@pytest.fixture
def order_book():
    return OrderBook()


def test_spread_record_format():
    orderbook_timestamp = datetime(2024, 5, 25, 12, 0, 0)
    book = "TestBook"
    bid = 10.0
    ask = 12.0
    spread = 20.0

    result = OrderBook.spread_record_format(orderbook_timestamp, book, bid, ask, spread)

    assert result == {
        "orderbook_timestamp": "2024-05-25T12:00:00",
        "book": "TestBook",
        "bid": 10.0,
        "ask": 12.0,
        "spread": 20.0
    }


def test_compute_spread_from_order_book_spread_calculation(order_book):
    mock_order_book = Mock()
    mock_bid1, mock_bid2 = Mock(), Mock()
    mock_bid1.price, mock_bid2.price = 10.0, 11.0
    mock_ask1, mock_ask2 = Mock(), Mock()
    mock_ask1.price, mock_ask2.price = 12.0, 13.0
    mock_order_book.bids = [mock_bid1, mock_bid2]
    mock_order_book.asks = [mock_ask1, mock_ask2]
    mock_order_book.updated_at = datetime(2024, 5, 25, 12, 0, 0)

    result = order_book.compute_spread_from_order_book("TestBook", mock_order_book)

    assert isinstance(result, dict)
    assert "orderbook_timestamp" in result
    assert "book" in result
    assert "bid" in result
    assert "ask" in result
    assert "spread" in result

    # Calculate the expected spread
    best_bid = max([mock_bid1.price, mock_bid2.price])
    best_ask = min([mock_ask1.price, mock_ask2.price])
    expected_spread = ((best_ask - best_bid) * 100) / best_ask

    # Check that the computed spread matches the expected spread
    assert result["spread"] == expected_spread



def test_get_book_order_spread_record(order_book, monkeypatch):
    # Create a mock order book response with objects that have a 'price' attribute
    mock_order_book = Mock()
    mock_bid1, mock_bid2 = Mock(), Mock()
    mock_bid1.price, mock_bid2.price = 10.0, 11.0
    mock_ask1, mock_ask2 = Mock(), Mock()
    mock_ask1.price, mock_ask2.price = 12.0, 13.0
    mock_order_book.bids = [mock_bid1, mock_bid2]
    mock_order_book.asks = [mock_ask1, mock_ask2]
    mock_order_book.updated_at = datetime(2024, 5, 25, 12, 0, 0)

    # Patch the 'extract_order_book' method to return the mock order book
    mock_extract_order_book = Mock(return_value=mock_order_book)
    monkeypatch.setattr(order_book, 'extract_order_book', mock_extract_order_book)

    # Call the method under test
    result, order_book_date = order_book.get_book_order_spread_record("TestBook")

    # Assert that the result is a dictionary and contains the expected keys
    assert isinstance(result, dict)
    assert "orderbook_timestamp" in result
    assert "book" in result
    assert "bid" in result
    assert "ask" in result
    assert "spread" in result


