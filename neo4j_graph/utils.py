def get_data_str(data_dict):
    """
    Serialize python dict to Neo4j Cypher format.
    :type data_dict: dict
    :return: data string in '{key: "value"}' format
    """
    return '{{{}}}'.format(
        ','.join([
            '{}: "{}"'.format(key, value) if isinstance(value, str) else
            '{}: {}'.format(key, value)
            for key, value in data_dict.items()
        ])
    )
