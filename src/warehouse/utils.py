import os
import yaml
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def load_config():
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'configs.yaml')
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
        return config


class WarehouseMovementsModel:

    def __init__(self):
        # Load the config.yaml file
        self.configs = load_config()

        self.output_dir = self.configs["movements_table_output_path"]
        table_name = self.configs["movements_output_table_name"] + ".csv"
        self.output_filepath = os.path.join(self.output_dir, table_name)

    def get_dataframes(self):
        """
        Read source dataframes
        """
        deposits_file_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), self.configs["deposits_source_path"]
        )
        withdrawals_file_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), self.configs["withdrawals_source_path"]
        )

        deposits_df = pd.read_csv(deposits_file_path)
        withdrawals_df = pd.read_csv(withdrawals_file_path)
        return deposits_df, withdrawals_df

    @staticmethod
    def standardize_fields_naming(dataframe: pd.DataFrame, action_type: str):
        """
        Standardize the field names of the DataFrame.
        """
        dataframe = dataframe.rename(columns={'id': 'movement_id'})
        dataframe['action'] = action_type
        return dataframe

    @staticmethod
    def add_action_and_interface(dataframe: pd.DataFrame, action_type: str, interface=None):
        """
        Add the 'action' and 'interface' columns to the DataFrame.
        """
        dataframe['action'] = action_type
        dataframe['interface'] = interface
        return dataframe

    def read_output_table(self):
        """
        Read the existing output table from a CSV file.
        """
        try:
            return pd.read_csv(self.output_filepath)
        except FileNotFoundError:
            # Define the columns for an empty DataFrame
            columns = ['user_id', 'action', 'tx_status', 'event_timestamp', 'currency', 'amount', 'movement_id', 'interface']
            return pd.DataFrame(columns=columns)

    @staticmethod
    def combine_dataframes(deposits_df, withdrawals_df, existing_movements_df):
        """
        Combine the deposits and withdrawals DataFrames with the existing output DataFrame.
        """
        combined_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
        combined_df = combined_df[
            ['user_id', 'action', 'tx_status', 'event_timestamp', 'currency', 'amount', 'movement_id', 'interface']]

        # Merge with existing output table
        updated_output_df = pd.concat(
            [existing_movements_df.set_index('movement_id'), combined_df.set_index('movement_id')]).reset_index()
        updated_output_df.drop_duplicates(subset=['movement_id'], keep='last', inplace=True)

        return updated_output_df

    def save_movements_table(self, dataframe: pd.DataFrame):
        # Create directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)

        dataframe.to_csv(self.output_filepath, index=False)


class WarehouseUserLoginEvents:
    def __init__(self):
        # Load the config.yaml file
        self.configs = load_config()

        self.event_df = pd.DataFrame()

        self.output_dir = self.configs["user_login_events_table_output_path"]
        table_name = self.configs["user_login_events_output_table_name"] + ".csv"
        self.output_filepath = os.path.join(self.output_dir, table_name)


    def get_dataframe(self):
        event_file_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), self.configs["event_source_path"]
        )

        self.event_df = pd.read_csv(event_file_path)

    def convert_to_datetime(self):
        self.event_df['event_timestamp'] = pd.to_datetime(self.event_df['event_timestamp'])

    def filter_events(self):
        self.event_df = self.event_df[self.event_df['event_name'].isin(['login', '2falogin', 'login_api'])]

    def pivot_data(self):
        self.pivot_table = self.event_df.pivot_table(
            index='user_id', columns='event_name', values='event_timestamp', aggfunc='max'
        )
        self.pivot_table.columns = ['last_' + col if col != 'user_id' else col for col in self.pivot_table.columns]

    def reset_index(self):
        self.pivot_table = self.pivot_table.reset_index()

    def save_output(self):
        # Create directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)

        self.pivot_table.to_csv(self.output_filepath, index=False)
