import pandas as pd


def process_hubspot_data(all_entries, fields):
    list_of_properties = []
    for entry in all_entries:
        properties = {}
        entry = entry.to_dict()
        for key in fields.keys():
            if isinstance(fields.get(key), list):
                value = entry[fields.get(key)[0]][fields.get(key)[1]]
            else:
                value = entry[fields.get(key)]
            properties.update({key: value})
        list_of_properties.append(properties)
    df = pd.DataFrame(list_of_properties)
    return df
