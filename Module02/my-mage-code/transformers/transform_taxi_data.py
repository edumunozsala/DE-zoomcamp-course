from pandas import DataFrame
import math

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def remove_rows(df: DataFrame) -> DataFrame:
    df = df[df['passenger_count']>0]
    df = df[df['trip_distance']>0]

    return df

@transformer
def transform_df(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        df (DataFrame): Data frame from parent block.

    Returns:
        DataFrame: Transformed data frame
    """
    # Remove rows
    df = remove_rows(df)
    # Create a new column
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    df['lpep_dropoff_date'] = df['lpep_dropoff_datetime'].dt.date
    # Rename columns to snake case
    df.columns = (df.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
                )

    return df


@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
    assert len(df[df['passenger_count']<=0]) == 0, 'Passenger column has values lower than 0'
    assert len(df[df['trip_distance']<=0]) == 0, 'Trip distance column has values lower than 0'
