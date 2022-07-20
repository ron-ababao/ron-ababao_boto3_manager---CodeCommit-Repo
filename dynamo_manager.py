import json
import logging
import random
import boto3
from datetime import datetime
from datetime import timezone
from time import sleep
from decimal import *
import operator as op
from boto3.dynamodb.conditions import Key, Attr

def create_dynamo_table(table_name, pk, pkdef):
    ddb = boto3.resource('dynamodb')
    table = ddb.create_table(
            TableName=table_name,
            KeySchema=pk,
            AttributeDefinitions=pkdef,
            ProvisionedThroughput={'ReadCapacityUnits': 5,'WriteCapacityUnits': 5,
            }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table

def get_dynamo_table(table_name):
    ddb = boto3.resource('dynamodb')
    return ddb.Table(table_name)
    
def create_product(category, sku, **item):
    table = get_dynamo_table('products-ron22')
    keys = {'category': category,'sku': sku,}
    item.update(keys)
    table.put_item(Item=item)
    return table.get_item(Key=keys)['Item']

def update_product(category, sku, **item):
    table = get_dynamo_table('products-ron22')
    keys = {'category': category,'sku': sku,}
    expr = ', '.join([f'{k}=:{k}' for k in item.keys()])
    vals = {f':{k}': v for k, v in item.items()}
    table.update_item(Key=keys,UpdateExpression=f'SET {expr}',ExpressionAttributeValues=vals,)
    return table.get_item(Key=keys)['Item']
    
def delete_product(category, sku):
    table = get_dynamo_table('products-ron22')
    keys = {'category': category,'sku': sku,}
    res = table.delete_item(Key=keys)
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    else:
        log.error(f'There was an error when deleting the product: {res}'
    )
    return False
    
def create_dynamo_items(table_name, items, keys=None):
    table = get_dynamo_table(table_name)
    params = {
        'overwrite_by_pkeys': keys
    } if keys else {}
    with table.batch_writer(**params) as batch:
        for item in items:
            batch.put_item(Item=item)
    return True

def query_products(key_expr, filter_expr=None):
    # Query requires that you provide the key filters
    table = get_dynamo_table('products-ron22')
    params = {
        'KeyConditionExpression': key_expr,
    }
    if filter_expr:
        params['FilterExpression'] = filter_expr
    res = table.query(**params)
    return res['Items']
    
def scan_products(filter_expr):
    # Scan does not require a key filter. It will go through
    # all items in your table and return all matching items.
    # Use with caution!
    table = get_dynamo_table('products-ron22')
    params = {'FilterExpression': filter_expr,}
    res = table.scan(**params)
    return res['Items']

def delete_dynamo_table(table_name):
    table = get_dynamo_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True

if __name__ == '__main__':
    
    dyna_create=create_dynamo_table('products-ron22',pk=[{'AttributeName': 'category','KeyType': 'HASH',},{'AttributeName': 'sku','KeyType': 'RANGE',},],pkdef=[{'AttributeName': 'category','AttributeType': 'S',},{'AttributeName': 'sku','AttributeType': 'S',},],)
    print(dyna_create)
    
    table=get_dynamo_table('products-ron22')
    print(table.item_count)


    product = create_product('clothing', 'woo-hoodie927',product_name='Hoodie',is_published=True,price=Decimal('44.99'),in_stock=True)
    print(product)

    product = update_product('clothing', 'woo-hoodie927', in_stock=False,price=Decimal('54.75'))
    print(product)
   
    deletion=delete_product('clothing', 'woo-hoodie927')
    print(deletion)
    
    items = []
    sku_types = ('woo', 'foo')
    category = ('apparel', 'clothing', 'jackets')
    status = (True, False)
    prices = (Decimal('34.75'), Decimal('49.75'), Decimal('54.75'))
    
    for id in range(200):
        id += 1
        items.append({'category': random.choice(category),'sku': f'{random.choice(sku_types)}-hoodie-{id}','product_name': f'Hoodie {id}','is_published': random.choice(status),'price': random.choice(prices),'in_stock': random.choice(status),})
    
    batching=create_dynamo_items('products-ron22', items, keys=['category', 'sku'])
    print(batching)
    
    items = query_products(Key('category').eq('apparel') & Key('sku').begins_with('woo'))
    print(len(items))
    items = query_products(Key('category').eq('apparel') & Key('sku').begins_with('foo'))
    print(len(items))
    items = query_products(Key('category').eq('apparel'))
    print(len(items))
    items = query_products(Key('category').eq('apparel') & Key('sku').begins_with('foo'),filter_expr=Attr('in_stock').eq(True))
    print(len(items))
    items = scan_products(Attr('in_stock').eq(True))
    print(len(items))
    items = scan_products(Attr('price').between(Decimal('30'), Decimal('40')))
    print(len(items))
    items = scan_products((Attr('in_stock').eq(True) & Attr('price').between(Decimal('30'), Decimal('40'))))
    print(len(items))
    
    eradication=delete_dynamo_table('products-ron22')
    if eradication:
        log.info("Table has been deleted")
    else:
        log.info("Table has not been deleted")
