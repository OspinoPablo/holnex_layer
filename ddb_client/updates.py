import logging
import uuid

from boto3 import resource
from datetime import datetime as dt, timezone as tz
from ddb_client.config import defaults, allowed_fields, allowed_fields_to_create
from ddb_client.constants import RESERVED_WORDS
from ddb_client.utils import (
    chunk,
    extract_keys,
    describe_schema, 
    get_nested_value,
    convert_to_field_type, 
    flatten_dict,
    require_table,
    normalize_allowed_fields
)

# declare a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = resource('dynamodb')


@require_table
def __build_update_params(table:str, params:dict, data:dict, **conditions):
    log_criticals = conditions.pop('log_criticals', True)
    log_qparams   = conditions.pop('log_qparams', False)
    log_schema    = conditions.pop('log_schema', False)
    log_keys      = conditions.pop('log_keys', False)

    qparams              = {}
    schema               = describe_schema(table)
    allowed_fields_typed = normalize_allowed_fields(allowed_fields)

    pk_name, pk_type               = (schema[table]['pk'], schema[table]['pk_type'])
    schema_sk_name, schema_sk_type = (schema[table]['sk'], schema[table]['sk_type'])

    if log_schema:
        logger.info(f'Schema: {schema}')

    if params:
        if table not in allowed_fields_typed:
            logger.error(f'[Updates] | Schema `{table}` not declared in allowed_fields')
            return qparams
        
        if any([ isinstance(v, dict) for v in data.values() ]):
            data = flatten_dict(data)
        
        allowed_table_fields = allowed_fields_typed[table]

        filtered_out_keys = [k for k in data if k not in allowed_table_fields]
        data = {k: v for k, v in data.items() if k in allowed_table_fields}
        
        # Validate keys
        if filtered_out_keys and log_criticals:
            logger.critical(f'Filtered fields not allowed: {filtered_out_keys}')

        # Validate value types
        invalid_type_fields = []

        for field, value in data.items():
            if value is None:
                continue  # REMOVE sentence always permitted

            allowed_types = allowed_fields_typed[table][field]

            if not isinstance(value, allowed_types):
                invalid_type_fields.append(
                    (field, type(value).__name__, [t.__name__ for t in allowed_types])
                )

        # Remove invalid types and continue with the permitted ones
        if invalid_type_fields:
            logger.critical(
                f'[Updates] | Invalid types detected: {invalid_type_fields}'
            )
            data = {
                k: v for k, v in data.items()
                if k not in [f[0] for f in invalid_type_fields]
            }
        
        val_fmt = lambda key: key.replace('.', '_')
        exp_fmt = lambda key, sep: (key.replace('.', f'.{sep}'), val_fmt(key))
        
        try:
            if pk_name not in params or (schema_sk_name and schema_sk_name not in params):
                raise ValueError(f'[Updates] | The keys `{pk_name}` or `{schema_sk_name}` not provided in the keys')
            
            keys = {
                pk_name: convert_to_field_type(params.pop(pk_name, 0), pk_type),
                **({schema_sk_name: convert_to_field_type(params.pop(schema_sk_name, 0), schema_sk_type)} if schema_sk_name else {})
            }
            if log_keys:
                logger.info(f'Keys Built: {keys}')

            fields_to_set    = {k: v for k, v in data.items() if v is not None}
            fields_to_remove = [k for k, v in data.items() if v is None]
            
            # Validate there is at least one field to edit
            if not fields_to_set and not fields_to_remove:
                logger.warning(f'[Updates] | No fields to update or remove')
                return {}
            
            # Update sentences init
            update_expressions = []
            
            # SET expression for normal value fields
            if fields_to_set:
                set_exp = ', '.join(['#k{} = :k{}'.format(*exp_fmt(k, '#k')) for k in fields_to_set])
                update_expressions.append('SET ' + set_exp)
            
            # REMOVE expression for fields with None provided values
            if fields_to_remove:
                remove_parts = []

                # Nestes attributes
                for k in fields_to_remove:
                    if '.' in k:
                        parts = k.split('.')
                        remove_parts.append('.'.join([f'#k{part}' for part in parts]))
                    else:
                        remove_parts.append(f'#k{k}')
                
                remove_exp = ', '.join(remove_parts)
                update_expressions.append('REMOVE ' + remove_exp)
            
            # Condition expression
            condition_exp   = ' & '.join(['#c{} = :c{}'.format(*exp_fmt(c, '#c')) for c in conditions])
            attribute_names = {}

            for field in data.keys():
                for part in field.split('.'):
                    attribute_names[f'#k{part}'] = part
            
            for cond_field in conditions.keys():
                for part in cond_field.split('.'):
                    attribute_names[f'#c{part}'] = part
            
            # Build ExpressionAttributeValues
            attribute_values = {f':k{val_fmt(k)}': v for k, v in fields_to_set.items()}
            attribute_values.update({f':c{val_fmt(c)}': v for c, v in conditions.items()})
            
            qparams = {
                'Key'                      : keys,
                'UpdateExpression'         : ' '.join(update_expressions),
                'ExpressionAttributeNames' : attribute_names,
                'ExpressionAttributeValues': attribute_values,
                **({'ConditionExpression'  : condition_exp} if condition_exp else {})
            }
            if not attribute_values:
                qparams.pop('ExpressionAttributeValues')
        
        except Exception as e:
            logger.error(f'[Updates - builder] | Error trying to perform an operation in DynamoDB')
            logger.error(f'[ERROR DETAILS]: Schema `{table}` params {qparams} ')
            raise e

    if log_qparams:
        logger.info(f'Query Built: {qparams}')

    return qparams


@require_table
def __build_delete_params(table:str, params:dict, return_values=True, log_qparams=False , log_schema=False):
    schema = describe_schema(table)

    pk_name, pk_type = (schema[table]['pk'], schema[table]['pk_type'])
    schema_sk_name, schema_sk_type = (schema[table]['sk'], schema[table]['sk_type'])

    if log_schema:
        logger.info(f'Schema: {schema}')
        logger.info(f'|pk| {pk_name} = {pk_value} {type(pk_value)}')
        logger.info(f'|sk| {schema_sk_name} = {sk_value} {type(sk_value.value if hasattr(sk_value, 'build') else sk_value)}')

    if pk_name not in params or (schema_sk_name and schema_sk_name not in params):
        raise ValueError(f'[Updates] | The keys `{pk_name}` or `{schema_sk_name}` not provider in the item')
    
    pk_value = convert_to_field_type(params.pop(pk_name, 0), pk_type)
    sk_value = convert_to_field_type(params.pop(schema_sk_name, 0), schema_sk_type)
    qparams  = {
        'Key': {
            pk_name : pk_value,
            **({ schema_sk_name :sk_value } if schema_sk_name and schema_sk_name != '' else {})
        },
        'ReturnValues': 'ALL_OLD' if return_values else 'NONE'
    }
    if log_qparams:
        logger.info(f'Query built: {qparams}')
    
    return qparams


@require_table
def __build_increase_query(table:str, params:dict, data:dict, log_qparams=False, log_schema=False):
    schema = describe_schema(table)

    pk_name, pk_type               = (schema[table]['pk'], schema[table]['pk_type'])
    schema_sk_name, schema_sk_type = (schema[table]['sk'], schema[table]['sk_type'])

    if table not in allowed_fields:
        logger.error(f'[Updates] | The schema `{table}` has not been declared in the `allowed_fields` in config.py')
        return qparams

    if pk_name not in params or (schema_sk_name and schema_sk_name not in params):
        raise ValueError(f'[Updates] | The keys `{pk_name}` or `{schema_sk_name}` not provided in the item')

    pk_value = convert_to_field_type(params.pop(pk_name, 0), pk_type)
    sk_value = convert_to_field_type(params.pop(schema_sk_name, 0), schema_sk_type)

    if log_schema:
        logger.info(f'Schema: {schema}')
        logger.info(f'|pk| {pk_name} = {pk_value} {type(pk_value)}')
        logger.info(f'|sk| {schema_sk_name} = {sk_value} {type(sk_value.value if hasattr(sk_value, 'build') else sk_value)}')

    # Add always a default value in case the field does not exists
    qparams = {
        'Key': { 
            pk_name: pk_value,
            **({schema_sk_name: sk_value} if schema_sk_name else {})
        },
        'ReturnValues': 'NONE',
        'ExpressionAttributeValues': {
            ':start': 0
        },
    }

    update_expressions = []
    condition_expressions = []

    for elem in [i for i in data if i in allowed_fields[table]]:

        increase = int(data[elem])
        
        field_ref = f'#{elem}' if elem in RESERVED_WORDS else elem
        value_ref = f':{elem}'

        # Construir UpdateExpression (Incremento o Decremento)
        update_expressions.append(f'{field_ref} = if_not_exists({field_ref}, :start) + {value_ref}')
        qparams['ExpressionAttributeValues'][value_ref] = increase

        # Obtener límites si existen
        min_value = params.pop(f'{elem}_min', None)
        max_value = params.pop(f'{elem}_max', None)

        # Condición: Asegurar que el atributo existe antes de comparar
        condition = []
        if min_value is not None and increase < 0:
            qparams['ExpressionAttributeValues'][f':{elem}_min'] = int(min_value)
            condition.append(f'attribute_exists({field_ref}) AND {field_ref} > :{elem}_min')


        if max_value is not None and increase > 0:
            qparams['ExpressionAttributeValues'][f':{elem}_max'] = int(max_value)
            condition.append(f'attribute_exists({field_ref}) AND {field_ref} < :{elem}_max')

        if condition:
            condition_expressions.append(' AND '.join(condition))

    qparams['UpdateExpression'] = 'SET ' + ', '.join(update_expressions)

    # Solo agregar ConditionExpression si hay límites definidos
    if condition_expressions:
        qparams['ConditionExpression'] = ' AND '.join(condition_expressions)

    # Manejo de nombres reservados
    if any(elem in RESERVED_WORDS for elem in data):
        qparams['ExpressionAttributeNames'] = {
            f'#{elem}': elem for elem in data if elem in RESERVED_WORDS
        }

    if log_qparams:
        logger.info(f'Query built: {qparams}')

    return qparams


def ddb_create(table: str, data: dict, **args):
    log_qparams = args.pop('log_qparams', True)
    log_errors  = args.pop('log_errors', True)

    schema       = describe_schema(table)
    dynamo_table = dynamodb.Table(table)

    pk_name, pk_type               = (schema[table]['pk'], schema[table]['pk_type'])
    schema_sk_name, schema_sk_type = (schema[table]['sk'], schema[table]['sk_type'])

    try:
        if table not in defaults['data']:
            raise ValueError(f'[CREATE] | Schema `{table}` not declared in defaults.data')

        if table not in allowed_fields_to_create:
            raise ValueError(f'[CREATE] | Schema `{table}` not declared in allowed_create_fields')

        if any(isinstance(v, dict) for v in data.values()):
            data = flatten_dict(data)

        allowed_table_fields = allowed_fields_to_create[table]

        filtered_out_keys = [k for k in data if k not in allowed_table_fields]
        data = {k: v for k, v in data.items() if k in allowed_table_fields}

        if filtered_out_keys:
            logger.critical(f'[CREATE] | Filtered fields not allowed: {filtered_out_keys}')

        invalid_type_fields = []

        for field, value in data.items():
            allowed_types = allowed_table_fields[field]
            if not isinstance(value, allowed_types):
                invalid_type_fields.append(
                    (field, type(value).__name__, [t.__name__ for t in allowed_types])
                )

        if invalid_type_fields:
            logger.critical(f'[CREATE] | Invalid types detected: {invalid_type_fields}')
            invalid_fields = [f[0] for f in invalid_type_fields]
            data = {k: v for k, v in data.items() if k not in invalid_fields}

        item = {
            'id': str(uuid.uuid4()),
            'timestamp': int(dt.now(tz.utc).timestamp() * 1000),
            'created_at': dt.now(tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            **defaults['data'][table],
            **data
        }
        if pk_name not in item or (schema_sk_name and schema_sk_name not in item):
            raise ValueError(f'[CREATE] | Keys `{pk_name}` or `{schema_sk_name}` missing')

        item.update({
            pk_name: convert_to_field_type(item.pop(pk_name), pk_type),
            **({schema_sk_name: convert_to_field_type(item.pop(schema_sk_name), schema_sk_type)} if schema_sk_name else {})
        })
        if log_qparams:
            logger.info(f'Item to create: {item}')

        result = dynamo_table.put_item(Item=item)

    except ValueError:
        logger.error(f'[CREATE] | Error performing operation in DynamoDB `{table}`', exc_info=log_errors)
        return {'status': False, 'item': 0}

    return {'data': item, 'status': result['ResponseMetadata']['HTTPStatusCode'] == 200}


def ddb_update(table:str, params:dict, data:dict, **args):
    log_errors = args.pop('log_errors', True)

    params = __build_update_params(table, params, data, **args)
    
    try:
        table = dynamodb.Table(table)
        result = table.update_item(**params)
    except Exception:
        logger.error(f'[UPDATE] | Error trying to perform an operation in DynamoDB `{table}` ', exc_info=log_errors)
        return { 'status' : False, 'row_count' : 0 }

    return { 'status' : result['ResponseMetadata']['HTTPStatusCode']==200, 'row_count' : 1 if result['ResponseMetadata']['HTTPStatusCode']==200 else 0 }


def ddb_increase(table:str, params:dict, data:dict, **args):
    log_errors   = args.pop('log_errors', True)
    dynamo_table = dynamodb.Table(table)
    
    try:
        qparams = __build_increase_query(table, params, data, **args)
        result  = dynamo_table.update_item(**qparams)

    except Exception as e:
        if 'ConditionalCheckFailedException' in str(e):
            logger.critical(e)
            logger.critical('Error evaluating condition. Possibly the range of allowed increments was reached')
        else: 
            logger.error(f'[INCREASE] | Error trying to perform an operation in DynamoDB `{table}`', exc_info=log_errors)

        return { 'status': False, 'row_count': 0 }
    
    return { 'status' : result['ResponseMetadata']['HTTPStatusCode']==200, 'row_count' : 1 if result['ResponseMetadata']['HTTPStatusCode']==200 else 0 }


def ddb_delete(table:str, params:dict, **args):
    log_errors   = args.pop('log_errors', True)
    dynamo_table = dynamodb.Table(table)
    qparams      = __build_delete_params(table, params, **args)
    
    try:
        result = dynamo_table.delete_item(**qparams)

    except ValueError:
        logger.error(f'[Updates] | Error trying to perform an operation in DynamoDB `{table}`', exc_info=log_errors)
        return { 'status' : False, 'row_count' : 0 }
    except Exception:
        logger.error(f'[Updates] | Error trying to perform an operation in DynamoDB `{table}`', exc_info=log_errors)
        return { 'status' : False, 'row_count' : 0 }
    
    return { 'status' : result['ResponseMetadata']['HTTPStatusCode']==200, 'row_count' : 1 if 'Attributes' in result else 0 }


# Batches 
@require_table
def batch_delete_items(table:str, source:list, **args):
    logger_keys   = args.pop('log_keys', False)
    logger_result = args.pop('log_result', False)

    schema = describe_schema(table)

    pk_name, pk_type = (schema[table]['pk'], schema[table]['pk_type'])
    schema_sk_name, schema_sk_type = (schema[table]['sk'], schema[table]['sk_type'])

    # Extract keys
    pk_table_params, pk_data, sk_table_params, sk_data = extract_keys(args)

    # Verify schema correspondence
    if pk_table_params != pk_name or (schema_sk_name and (schema_sk_name != sk_table_params)):
        logger.error(f'[Updates] | Key mismatch error detected for table `{table}`.')
        
        if pk_table_params != pk_name:
            logger.error(f'[Updates] | Expected PK: `{pk_name}`, Found PK: `{pk_table_params}`')

        if schema_sk_name:
            logger.error(f'[Updates] | Expected SK: `{schema_sk_name}`, Found SK: `{sk_table_params}`')
        else:
            logger.error(f'[Updates] | No Sort Key (SK) was expected, but a mismatch was detected.')

        return {'status': False, 'row_count': 0}

    keys           = []  
    seen_item_data = set() 

    for item in source:
        pk_value   = convert_to_field_type(get_nested_value(item, pk_data.split('.')), pk_type)
        sk_value   = convert_to_field_type(get_nested_value(item, sk_data.split('.')), schema_sk_type) if schema_sk_name else None
        unique_key = (pk_value, sk_value) if schema_sk_name else (pk_value,)

        if unique_key not in seen_item_data:
            if pk_value:
                if not schema_sk_name or sk_value:
                    keys_to_delete = {
                        pk_name: pk_value,
                        **({schema_sk_name: sk_value} if schema_sk_name else {})
                    }
                    keys.append({'DeleteRequest': {'Key': keys_to_delete}})
                    seen_item_data.add(unique_key)

    if logger_keys:
        logger.info(f'Keys to delete: {keys}')

    # Init limits and response attributes
    max_batch_size = 25  
    row_count      = 0  

    for batch in chunk(keys, max_batch_size):
        request_items = { table: batch }

        response   = dynamodb.batch_write_item(RequestItems=request_items)
        row_count += len(batch)  # Incrementar el contador con el tamaño del batch

        if logger_result:
            logger.info(f'Delete Response: {response}')

        while 'UnprocessedItems' in response and response['UnprocessedItems']:
            unprocessed_items = response['UnprocessedItems'][table]
            request_items     = { table: unprocessed_items }
            response          = dynamodb.batch_write_item(RequestItems=request_items)

    return {'status': row_count > 0, 'row_count': row_count}


@require_table
def batch_create_items(table: str, source:list, **args):
    logger_keys   = args.pop('log_keys', False)
    logger_result = args.pop('log_result', False)

    schema = describe_schema(table)

    pk_name, pk_type               = (schema[table]['pk'], schema[table]['pk_type'])
    schema_sk_name, schema_sk_type = (schema[table]['sk'], schema[table]['sk_type'])

    # Extract keys
    pk_table_params, pk_data, sk_table_params, sk_data = extract_keys(args)

    # Verify schema correspondence
    if pk_table_params != pk_name or (schema_sk_name and (schema_sk_name != sk_table_params)):
        logger.error(f'[Updates] | Key mismatch error detected for table `{table}`.')

        if pk_table_params != pk_name:
            logger.error(f'[Updates] | Expected PK: `{pk_name}`, Found PK: `{pk_table_params}`')

        if schema_sk_name:
            logger.error(f'[Updates] | Expected SK: `{schema_sk_name}`, Found SK: `{sk_table_params}`')
        else:
            logger.error(f'[Updates] | No Sort Key (SK) was expected, but a mismatch was detected.')

        return {'status': False, 'row_count': 0}

    # Init items to insert and remove duplicates
    items = []
    seen_item_data = set()

    if table not in defaults.get('data', {}):
        logger.error(f'[Updates] | The schema `{table}` has not been declared in the `defaults.data` in config.py')

        return { 'status': False, 'row_count': 0 }

    for item in source:
        pk_value   = convert_to_field_type(get_nested_value(item, pk_data.split('.')), pk_type)
        sk_value   = convert_to_field_type(get_nested_value(item, sk_data.split('.')), schema_sk_type) if schema_sk_name else None
        unique_key = (pk_value, sk_value) if schema_sk_name else (pk_value,)

        if unique_key not in seen_item_data:
            if pk_value:
                if not schema_sk_name or sk_value:
                    item_to_insert = {
                        pk_name: pk_value,
                        **({schema_sk_name: sk_value} if schema_sk_name else {}),
                        **(defaults['data'][table] if table in defaults['data'] else {}),
                        **({
                            'id': str(uuid.uuid4()),
                            'timestamp': int(dt.now(tz.utc).timestamp() * 1000),
                            'created_at': dt.now(tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        }),
                        **item  # Incluir el resto del contenido del item
                    }
                    items.append({'PutRequest': {'Item': item_to_insert}})
                    seen_item_data.add(unique_key)

    if logger_keys:
        logger.info(f'Items to insert: {items}')

    # Init limits and response attributes
    max_batch_size = 25
    row_count      = 0      

    for batch in chunk(items, max_batch_size):
        request_items = {table: batch}
        response      = dynamodb.batch_write_item(RequestItems=request_items)
        row_count    += len(batch)

        if logger_result:
            logger.info(f'Insert Response: {response}')

        while 'UnprocessedItems' in response and response['UnprocessedItems']:
            unprocessed_items = response['UnprocessedItems'][table]
            request_items = {table: unprocessed_items}
            response = dynamodb.batch_write_item(RequestItems=request_items)

    return {'status': row_count > 0, 'row_count': row_count}