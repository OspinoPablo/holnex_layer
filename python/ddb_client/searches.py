import logging
import config

from functools import reduce
from config import fields
from constants import RESERVED_WORDS
from utils import (
    chunk, 
    extract_keys,
    describe_schema, 
    show_schema_details,
    get_nested_value,
    convert_to_field_type, 
    buildProjectionExpression, 
    extract_search_structure,
    require_table,
    convert_decimals
)
from boto3 import resource
from boto3.dynamodb.conditions import Key, Attr, And

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = resource('dynamodb')


@require_table
def __build_search_params(table, params, paginate, index=None, order='ASC', pages=config.pages, t='list', order_by=None, log_qparams=False, log_schema=False):
    keys = describe_schema(table)

    limit              = paginate
    scan_index_forward = True if order=='ASC' else False
    
    #selecting fields
    select = params.pop('fields', fields[table] if table in fields else '')

    try: 
        # Table index
        pk_name, pk_type = (keys[table]['pk'], keys[table]['pk_type']) if not index else (keys[table][index]['pk'], keys[table][index]['pk_type'])
        sk_name, sk_type = (keys[table]['sk'], keys[table]['sk_type']) if not index else (keys[table][index]['sk'], keys[table][index]['sk_type'])

        if pk_name not in params:
            raise KeyError(f'{pk_name}')
        
    except KeyError as e:
        logger.error(f'ddb_client.searches | The {e} key or index is not found in the {table} schema')
        schema_keys = show_schema_details(table, keys)
        logger.critical(f'The indices of the scheme are {schema_keys}')
        return None
    
    pk_value = convert_to_field_type(params.pop(pk_name, 0), pk_type)
    sk_value = params.pop(sk_name, None)
    
    sk_expression = None
    if sk_value is not None:
        if hasattr(sk_value, 'build'):
            # Use the custom operator from DynamoComparison
            sk_expression = sk_value.build(sk_name, sk_type)
        else:
            # Default to 'eq' and infer type
            sk_expression = Key(sk_name).eq(convert_to_field_type(sk_value, sk_type))

    if order_by:
        order_by = next((lsi for lsi in keys[table].get('LSI', []) if order_by in lsi), None)

    if log_schema:
        logger.info(f'Schema: {keys}')
        logger.info(f'|pk| {pk_name} = {pk_value} {type(pk_value)}')
        logger.info(f'|sk| {sk_name} = {sk_value} {type(sk_value.value if hasattr(sk_value, 'build') else sk_value)}')
    
    qparams = {
        'Select': 'COUNT' if t=='count' else 'ALL_ATTRIBUTES' if select=='' else 'SPECIFIC_ATTRIBUTES',
        
        # Busqueda por Pk
        **({'KeyConditionExpression': Key(pk_name).eq(pk_value)} if not sk_expression else {}),
        
        # Busqueda por Pk y Sort Key con operador dinámico
        **({'KeyConditionExpression': Key(pk_name).eq(pk_value) & sk_expression} if sk_expression else {}),
        
        # Indicar que se buscará por indice
        **({'IndexName': '{}'.format(index)} if index else {}),
        
        # Indice para organizar los datos
        **({'IndexName': '{}'.format(order_by)} if order_by else {}),
        
        **({'Limit': pages} if limit else {}),
        
        'ConsistentRead': bool(not(index)),
        'ScanIndexForward': scan_index_forward
    }

    
    if t == 'list':
        last_evaluated_key = params.pop('LastEvaluatedKey', '')
        
        if last_evaluated_key != '':
            qparams['ExclusiveStartKey'] = last_evaluated_key
        if select != '':
            qparams['ProjectionExpression'] = ','.join(['#'+str(elem).replace('.', '.#') if str(elem) in RESERVED_WORDS else str(elem) for elem in select])
            
            if qparams['ProjectionExpression'].find('#') != -1:
                qparams['ExpressionAttributeNames'] = {}
                for elem in select:
                    if str(elem) in RESERVED_WORDS:
                        for sub_elem in str(elem).split('.'):
                            qparams['ExpressionAttributeNames']['#'+str(sub_elem)] = str(sub_elem)

    #building where    
    filter_expression_list = []
    
    for field, value in params.items():
        if hasattr(value, 'build'):
            expression = value.build(field) 
        else:
            expression = Attr(field).eq(value if isinstance(value, int) else str(value))
        
        filter_expression_list.append(expression)
    
    if len(filter_expression_list) > 1:
        qparams['FilterExpression'] =  reduce(And, filter_expression_list)

    elif len(filter_expression_list) == 1:
        qparams['FilterExpression'] = filter_expression_list[0]

    if log_qparams:
        structure = extract_search_structure(qparams)
        logger.info(f'Log Structure: {structure}')
    
    return qparams


@require_table
def ddb_read(table:str, params:dict, limit=config.paginate, **args):
    log_errors = args.pop('log_errors', True)
    log_search = args.pop('log_search', False)
    pages      = args.get('pages', config.pages)

    if log_search:
        logger.info(f'{table} => {params}')
    
    result = {}
    items  = []
    
    dynamo_table = dynamodb.Table(table)
    qparams = __build_search_params(table, params, paginate=limit, **args)
    
    if qparams: 
        while True:
            try:
                result = dynamo_table.query(**qparams)
                items+= result['Items']
                
                if(not(limit) or len(items) < pages):
                    qparams['ExclusiveStartKey']=result.pop('LastEvaluatedKey')
                else:
                    break
            except KeyError as e:
                break
            except Exception as e:
                logger.error(f'[READ - KeyError] | Error trying to perform an operation in DynamoDB', exc_info=log_errors)
                logger.error(f'[ERROR DETAILS]: Schema `{table}` params {qparams}')
                break

    return { 'Items': convert_decimals(items), 'Count_Items' :len(items), **({'LastEvaluatedKey':result['LastEvaluatedKey']} if 'LastEvaluatedKey' in result else {}) }


@require_table
def batch_get_items(table:str, source:list, **args):
    xfields       = args.pop('fields', [])
    logger_keys   = args.pop('log_keys', False)
    logger_result = args.pop('log_result', False)

    schema = describe_schema(table)

    pk_name, pk_type = (schema[table]['pk'], schema[table]['pk_type'])
    sk_name, sk_type = (schema[table]['sk'], schema[table]['sk_type'])
    
    # Key extraction
    pk_table_params, pk_data, sk_table_params, sk_data = extract_keys(args)

    # Build unique_keys
    keys  = []            
    items = []            
    max_batch_size = 100  
    seen_item_data = set()

    if pk_table_params != pk_name or (sk_name and (sk_name != sk_table_params)):
        logger.error(f'ddb_client.searches | Key mismatch error detected for table `{table}`.')
        
        if pk_table_params != pk_name:
            logger.error(f'ddb_client.searches | Expected PK: `{pk_name}`, Found PK: `{pk_table_params}`')

        if sk_name:
            logger.error(f'ddb_client.searches | Expected SK: `{sk_name}`, Found SK: `{sk_table_params}`')
        else:
            logger.error('ddb_client.searches | No Sort Key (SK) was expected, but a mismatch was detected.')

        return items

    # Response fields
    select = xfields if xfields else fields[table] if table in fields else ''

    for item in source:
        if sk_name and not sk_data:
            continue

        # Obtener valores de pk y sk considerando niveles anidados
        pk_value =  convert_to_field_type(get_nested_value(item, pk_data.split('.')) , pk_type)  # Convertir la pk en una lista si está anidada
        sk_value =  convert_to_field_type(get_nested_value(item, sk_data.split('.')), sk_type) if sk_name else None

        # Construir la clave única
        unique_key = (pk_value, sk_value) if sk_name else (pk_value,)

        # Validar si debe agregarse a keys
        if unique_key not in seen_item_data:
            if pk_value:  # Siempre se requiere pk
                # Si se requiere sk, debe estar presente; si no, se ignora
                if (not sk_name) or (sk_name and sk_value):
                    keys.append({
                        pk_name: pk_value, 
                        **({sk_name: sk_value} if sk_name else {})
                    })
                    seen_item_data.add(unique_key)

    if logger_keys:
        logger.info(f'Keys Built {keys}')
        
        
    projection_expression = buildProjectionExpression(select)

    # Dividir las claves en lotes de 100
    for batch in chunk(keys, max_batch_size):
        
        request_items = { table: { 'Keys': batch, **projection_expression }}
        
        response = dynamodb.batch_get_item(RequestItems=request_items)
        
        if logger_result:
            logger.info(f'Response {response}')
        
        # Agregar elementos recuperados
        items.extend(response['Responses'].get(table, []))
        
        # Manejar elementos no procesados
        while 'UnprocessedKeys' in response and response['UnprocessedKeys']:
            unprocessed_keys = response['UnprocessedKeys'][table]['Keys']
            response = dynamodb.batch_get_item(RequestItems={
                table: {'Keys': unprocessed_keys}
            })
            items.extend(response['Responses'].get(table, []))
        
        
    return convert_decimals(items)



@require_table
def ddb_counter(table:str, params:dict, **args):
    log_errors = args.pop('log_errors', True)

    pages   = 1
    amount  = 0
    qparams = __build_search_params(table, params, paginate=False, t='count', **args)
    
    dynamo_table = dynamodb.Table(table)

    while True:
        try:
            result  = dynamo_table.query(**qparams)
            amount += int(result['Count'])
            qparams['ExclusiveStartKey'] = result['LastEvaluatedKey']

        except KeyError as ke:
            logger.error(f'[COUNTER - KeyError]:: {ke}')
            break
        except Exception as e:
            logger.error(f'[COUNTER - KeyError] | Error trying to perform an operation in DynamoDB', exc_info=log_errors)
            logger.error(f'[ERROR DETAILS]: Schema `{table}` params {qparams} ')
            break
    
    if config.pages > 0:
        pages = ( amount / config.pages )

    pages +=  1 if pages % 1 > 0 else 0

    return { 'Count_Items': amount, 'Pages': int(pages) }


@require_table
def dynamo_scan(table: str):
    # Escanear tabla records_links
    dynamo_table = dynamodb.Table(table)
    items = []

    response = dynamo_table.scan()
    items.extend(response['Items'])

    while 'LastEvaluatedKey' in response:
        response = dynamo_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    return convert_decimals(items)