import sys
from utils import WarehouseMovementsModel


def main(args):
    # WarehouseMovementsModel Util
    warehouse_movements_model = WarehouseMovementsModel()

    # Get input tables
    deposits_df, withdrawals_df = warehouse_movements_model.get_dataframes()

    # Standardize column_names and movement identifier
    deposits_df = warehouse_movements_model.standardize_fields_naming(deposits_df, 'deposit')
    withdrawals_df = warehouse_movements_model.standardize_fields_naming(withdrawals_df, 'withdrawal')

    # Add Action and Interface Fields
    deposits_df = warehouse_movements_model.add_action_and_interface(deposits_df, 'deposit', None)
    withdrawals_df = warehouse_movements_model.add_action_and_interface(withdrawals_df, 'withdrawal',
                                                                        withdrawals_df['interface'])

    # Merge Tables between them and previous output
    existing_movements_df = warehouse_movements_model.read_output_table()
    movements_df = warehouse_movements_model.combine_dataframes(
        deposits_df=deposits_df,
        withdrawals_df=withdrawals_df,
        existing_movements_df=existing_movements_df
    )

    # Save the updated output table to the file
    warehouse_movements_model.save_movements_table(dataframe=movements_df)


if __name__ == "__main__":
    main(sys.argv[1:])
