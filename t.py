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
print(merged_dict)
