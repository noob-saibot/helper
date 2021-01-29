# Columns: User, Store, Amount
store_seq = df.select('Store').distinct().collect()

def create_map(rdd):
    return (rdd
            .groupBy(lambda _: _[0])
            .map(lambda _: (_[0], [{val['Store']: val['Amount'] for val in _[1]}])))

def create_vector(cat_maps):
    return (cat_maps
            .map(lambda cat: (cat[0], Vectors.dense([cat[1][0].get(val.Store, 0) for val in store_seq])))
            .toDF(['User', 'Amount_as_vector']))

result = create_vector(create_map(df.select('User', 'Store', 'Amount').rdd))
