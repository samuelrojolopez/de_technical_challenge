import os
import sys
import pytest
import pandas as pd

# Add the root directory of your project to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.warehouse.utils import WarehouseMovementsModel


@pytest.fixture
def deposits_df():
    return pd.DataFrame({
        'event_id': [482897, 255617, 93563, 418028, 567060],
        'event_timestamp': ["2022-11-01 15:40:24.482+00", "2022-06-24 21:51:24.53+00", "2022-05-25 23:00:29.611+00",
                            "2022-10-01 06:38:24.192+00", "2022-11-24 04:50:02.619+00"],
        'user': ["5c04925674920eb58467fb52ce4ef728", "6578ea8c336aa704c7e8ea2c5f19353b",
                 "6578ea8c336aa704c7e8ea2c5f19353b", "c4ef9c39b300931b69a36fb3dbb8d60e",
                 "5c96ab47cd8c08e27bb5e3e8e7cc72c4"],
        'amount': [5240.02914542, 0.15, 0.01, 224827.14518, 1559177.0700000001],
        'currency': ["usd", "btc", "btc", "brl", "ars"],
        'status': ["complete", "complete", "complete", "complete", "complete"],
        'action': ["deposit", "deposit", "deposit", "deposit", "deposit"],
        'interface': [None, None, None, None, None]
    })


@pytest.fixture
def withdrawals_df():
    return pd.DataFrame({
        'event_id': [3867, 64463, 44448, 48922, 30604],
        'event_timestamp': ['2020-05-18 22:01:33.759+00', '2022-06-02 18:06:50.684+00', '2021-07-14 15:43:50.075+00',
                            '2021-10-19 19:14:25.606+00', '2021-05-18 21:36:54.731+00'],
        'user': ['130f1a8e9e102707f3f91b010f151b0b', 'e70611883d2760c8bbafb4acb29e3446',
                 '32c3215de8110a85d451202e083fe156', '0197fcd95060131d9bc5e564f842ed53',
                 '60e4ac6a656ef4b4fd82aaaf25f14736'],
        'amount': [10.0, 5.0, 10.0, 201.0, 123.0],
        'interface': ['web', 'web', 'web', 'web', 'web'],
        'currency': ['mxn', 'brl', 'mxn', 'mxn', 'brl'],
        'status': ['complete', 'complete', 'complete', 'complete', 'complete'],
        'action': ['withdrawal', 'withdrawal', 'withdrawal', 'withdrawal', 'withdrawal']
    })


@pytest.fixture
def existing_movements_df():
    return pd.DataFrame(
        columns=['movement_id', 'user_id', 'action', 'tx_status', 'event_timestamp', 'currency', 'amount', 'interface'])


def test_standardize_fields_naming_deposits(deposits_df):
    standardized_df = WarehouseMovementsModel.standardize_fields_naming(deposits_df, 'test_action')
    assert 'event_id' in standardized_df.columns
    assert 'action' in standardized_df.columns
    assert standardized_df['action'].unique() == ['test_action']
    assert list(standardized_df.columns) == [
        'event_id', 'event_timestamp', 'user', 'amount', 'currency', 'status', 'action', 'interface'
    ]


def test_standardize_fields_naming_withdrawals(withdrawals_df):
    standardized_df = WarehouseMovementsModel.standardize_fields_naming(withdrawals_df, 'test_action')
    assert 'event_id' in standardized_df.columns
    assert 'action' in standardized_df.columns
    assert standardized_df['action'].unique() == ['test_action']
    assert list(standardized_df.columns) == [
        'event_id', 'event_timestamp', 'user', 'amount', 'interface', 'currency', 'status', 'action'
    ]


def test_add_action_and_interface_deposits(deposits_df):
    augmented_df = WarehouseMovementsModel.add_action_and_interface(
        deposits_df, 'test_action', 'test_interface'
    )
    assert 'action' in augmented_df.columns
    assert 'interface' in augmented_df.columns
    assert augmented_df['action'].unique() == ['test_action']
    assert augmented_df['interface'].unique() == ['test_interface']


def test_add_action_and_interface_withdrawals(withdrawals_df):
    augmented_df = WarehouseMovementsModel.add_action_and_interface(
        withdrawals_df, 'test_action', 'test_interface'
    )
    assert 'action' in augmented_df.columns
    assert 'interface' in augmented_df.columns
    assert augmented_df['action'].unique() == ['test_action']
    assert augmented_df['interface'].unique() == ['test_interface']
