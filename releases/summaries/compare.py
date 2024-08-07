#!/usr/bin/python
import json

# SO HACKY
file1 = '2024mar24.json'
file2 = '2024jul13.json'

with open(file1, 'r') as f:
    summary1 = json.load(f)

with open(file2, 'r') as f:
    summary2 = json.load(f)

# Step 1. Get a list of all the fields by reducing two-step fields.
def get_fields_and_values(summary, prefix=''):
    results = {}

    for key in summary:
        value = summary[key]
        if isinstance(value, dict):
            inner_dict = get_fields_and_values(value, key)
            for key in inner_dict:
                results[f"{prefix}.{key}"] = inner_dict[key]
        else:
            results[f"{prefix}.{key}"] = value

    return results

fieldsvalues1 = get_fields_and_values(summary1)
fieldsvalues2 = get_fields_and_values(summary2)

all_fields = sorted(fieldsvalues1.keys() | fieldsvalues1.keys())

print(f"field\t{file1}\t{file2}\tdiff")
for field in all_fields:
    val1 = fieldsvalues1.get(field, '')
    val2 = fieldsvalues2.get(field, '')
    diff = ''
    if isinstance(val1, int) and isinstance(val2, int):
        diff = val2 - val1
    print(f"{field[1:]}\t{val1}\t{val2}\t{diff}")
