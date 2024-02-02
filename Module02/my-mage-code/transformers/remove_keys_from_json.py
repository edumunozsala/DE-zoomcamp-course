if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # Set the keys to keep
    keys_to_remove = ['features', 'jurisdiction_names','license', 'color_summary',
                     'picture_url', 'medium_url', 'thumbnail_url']
    
    transformed_data = [{k:v for k, v in d.items() if k not in keys_to_remove}
                        for d in data
                        ]

    return transformed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert len(output)>0, 'The output is empty'
