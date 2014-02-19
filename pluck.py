def validate(fields):
    # [1, 2, 3, {'a': ['1', '2', '3', {'b': ['1', '2', '3']}]}]
    for field in fields:
        # 1 or {'a': [...]}
        if type(field) is dict:
            # {'a': [...]}
            for key in field:
                # 'a'
                print key
                validate(key, field[key])