from collections import defaultdict

def merge_dicts(dict1, dict2):
    result = {}
    for key, value in dict1.items():
        result[key] = result.setdefault(key, 0) + value
    for key, value in dict2.items():
        result[key] = result.setdefault(key, 0) + value
    return result

dict1 = {'a': 1, 'b': 2, 'c': 3}
dict2 = {'b': 3, 'd': 4}

merged_dict = merge_dicts(dict1, dict2)
# print(merged_dict)

dict1 = {'a': {'x': 1, 'y': 2}, 'b': {'x': 3, 'y': 4}}
dict2 = {'a': {'x': 5, 'y': 6}, 'c': {'x': 7, 'y': 8}}

merged_dict = defaultdict(lambda: defaultdict(int))

for d in (dict1, dict2):
    for key, value in d.items():
        for inner_key, inner_value in value.items():
            merged_dict[key][inner_key] += inner_value

print(dict(merged_dict))