import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    city = kwargs['configuration'].get('city')
    url = f'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/airbnb-listings/records?where=city="{city}"'
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the JSON response
        return response.json()["results"]
        # Return the dataframe
    else:
        return []        
    #return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert len(output)>0, 'The output is empty'
