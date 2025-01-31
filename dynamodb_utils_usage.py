# Initialize the utility class
db_utils = DynamoDBUtils('your-table-name', 'us-west-2')

# Create an item
item = {
    'id': '123',
    'name': 'Test Item',
    'data': {'key': 'value'}
}
db_utils.put_item(item)

# Get an item
result = db_utils.get_item({'id': '123'})

# Update an item
db_utils.update_item(
    {'id': '123'},
    {'name': 'Updated Name', 'data': {'new': 'value'}}
)

# Query items
results = db_utils.query_items(
    'id = :id',
    {':id': '123'}
)