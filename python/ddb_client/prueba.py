from searches import batch_get_items, ddb_read

source = [
    {'category_id': '1a2b3c4d-5678-90ab-cdef-1234567890ab', 'id': '2e39b421-2b61-4204-ba51-607adb693e3a'},
    {'category_id': '1a2b3c4d-5678-90ab-cdef-1234567890ab', 'id': '49621dd1-adf0-499c-892b-52bdb7cf55cc'},
    {'category_id': '1a2b3c4d-5678-90ab-cdef-1234567890ab', 'id': 'f72552b5-7ffa-4c42-a5f9-5ae940562b91'}
]

products = batch_get_items(
    table='core_products',
    source=source,
    category_id='category_id',
    id='id'
)

print(products)