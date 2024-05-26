import sys
from utils import WarehouseUserLoginEvents


def main(args):
    warehouse_events = WarehouseUserLoginEvents()

    # Get input tables
    warehouse_events.get_dataframe()

    # Make transformations to event login table
    warehouse_events.convert_to_datetime()
    warehouse_events.filter_events()
    warehouse_events.pivot_data()
    warehouse_events.reset_index()
    warehouse_events.save_output()


if __name__ == "__main__":
    main(sys.argv[1:])
