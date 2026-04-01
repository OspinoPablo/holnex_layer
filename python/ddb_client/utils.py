import logging
import ast

from itertools import islice
from boto3 import client

from ddb_client.constants import RESERVED_WORDS

# declare a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamoclient = client('dynamodb')

def chunk(iterable, size):
    """Divide una lista en sublistas de tamaño `size`."""
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, size)), [])


def buildProjectionExpression(select):

    """Construye una expresión de proyección para DynamoDB."""
    qparams = {}
    if bool(select):
        qparams['ProjectionExpression'] = ','.join(['#'+str(elem).replace(".", ".#") if str(elem) in RESERVED_WORDS else str(elem) for elem in select])
        if '#' in qparams['ProjectionExpression']:
            qparams['ExpressionAttributeNames'] = {
                f"#{sub_elem}": sub_elem for elem in select if elem in RESERVED_WORDS for sub_elem in str(elem).split(".")
            }
    return qparams


def describe_schema(table):
    
    SCHEMA = dynamoclient.describe_table(TableName=table)
    
    table_name = SCHEMA['Table']['TableName']
    key_schema = SCHEMA['Table']['KeySchema']
    attribute_definitions = { attr['AttributeName']: attr['AttributeType'] for attr in SCHEMA['Table']['AttributeDefinitions']}
    global_secondary_indexes = SCHEMA['Table'].get('GlobalSecondaryIndexes', [])
    local_secundary_indexes = SCHEMA['Table'].get('LocalSecondaryIndexes', [])  
    # Construir esquema base
    schema = {
        table_name: {
            'pk': None,  # HASH key
            'pk_type': None,  # Tipo de HASH key
            'sk': None,  # RANGE key
            'sk_type': None,  # Tipo de RANGE key
            'attributes_definitions': attribute_definitions
        }
    }

    # Extraer la clave primaria de la tabla
    for key in key_schema:
        if key['KeyType'] == 'HASH':
            schema[table_name]['pk'] = key['AttributeName']
            schema[table_name]['pk_type'] = attribute_definitions.get(key['AttributeName'])
        elif key['KeyType'] == 'RANGE':
            schema[table_name]['sk'] = key['AttributeName']
            schema[table_name]['sk_type'] = attribute_definitions.get(key['AttributeName'])

    # Procesar índices secundarios globales (GSI)
    for gsi in global_secondary_indexes:
        index_name = gsi['IndexName']
        gsi_schema = {
            'pk': None,  # HASH key
            'pk_type': None,  # Tipo de HASH key
            'sk': None,  # RANGE key (opcional)
            'sk_type': None  # Tipo de RANGE key (opcional)
        }
        for key in gsi['KeySchema']:
            if key['KeyType'] == 'HASH':
                gsi_schema['pk'] = key['AttributeName']
                gsi_schema['pk_type'] = attribute_definitions.get(key['AttributeName'])
            elif key['KeyType'] == 'RANGE':
                gsi_schema['sk'] = key['AttributeName']
                gsi_schema['sk_type'] = attribute_definitions.get(key['AttributeName'])
        
        # Agregar el índice al esquema
        schema[table_name][index_name] = gsi_schema

    schema[table_name]['LSI'] = []
    for lsi in local_secundary_indexes:
        index_name = lsi['IndexName']
        if index_name:
            schema[table_name]['LSI'].append(index_name)

    return schema
    

def show_schema_details(table_name, schema):
    # Extraer los valores principales
    table = schema[table_name]
    
    pk = f"{table['pk']} ({table['pk_type']})"
    sk = f"{table['sk']} ({table['sk_type']})" if table.get("sk") else "Not exist"

    # Identificar los índices secundarios globales (GSI)
    gsi = {}
    for key, value in table.items():
        if value:
            if isinstance(value, dict) and "pk" in value or "sk" in value:
                gsi[key] = "GSI"

    # Extraer los índices secundarios locales (LSI)
    lsi = table.get("LSI", [])

    # Crear el resultado formateado
    formatted_output = {
        "pk": pk,
        "sk": sk,
        **gsi,  # Agregar GSIs al diccionario
        "LSI": lsi if lsi else "Not exist"
    }

    return formatted_output


def convert_to_field_type(value, dynamo_type):

    """
    Convierte un valor al tipo correspondiente de DynamoDB.
    
    :param value: El valor a convertir.
    :param dynamo_type: El tipo DynamoDB ('S', 'N', 'B').
    :return: El valor convertido.
    """
    
    if isinstance(value, list):
        return value

    if value:
        if dynamo_type == 'S':  # String
            return str(value)
        elif dynamo_type == 'N':  # Number
            return int(value) if isinstance(value, int) else int(value)
        elif dynamo_type == 'B':  # Binary
            return bytes(value, 'utf-8') if isinstance(value, str) else value
        else:
            raise ValueError(f"ddb_client.utils | Unsupported DynamoDB type: {dynamo_type}")
            
    return value


def get_nested_value(data, keys):
    """
    Obtiene un valor anidado de un diccionario dado un recorrido de claves.
    :param data: El diccionario en el que se buscará.
    :param keys: Lista de claves que representan el recorrido.
    :return: El valor encontrado o None si alguna clave no existe.
    """
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return None
        data = data[key]
    return data


def extract_keys(keys_fields):
    """
    Extrae las claves primarias (pk) y secundarias (sk) de un diccionario.
    
    :param keys_fields: Diccionario con las claves proporcionadas.
    :return: (pk_name, pk, sk_name, sk)
    """
    keys_list = list(keys_fields.items())
    if len(keys_list) == 0:
        raise ValueError("ddb_client.utils | At least a primary key (pk) must be provided.")

    pk_name, pk = keys_list[0]
    sk_name, sk = keys_list[1] if len(keys_list) > 1 else (None, None)

    return pk_name, pk, sk_name, sk


def flatten_dict(data, parent_key = ''):
            
    result = {}
    
    for k, v in data.copy().items():
        
        key = f'{parent_key}.{k}' if parent_key else k
        
        if v and isinstance(v, dict):
            
            result.update(flatten_dict(v, key))
            continue
        
        result[key] = v
        
    return result



def extract_search_structure(qparams):
    """
    Construye una estructura legible y plana de qparams para debug/log.
    Convierte los árboles de condiciones en listas de dicts estilo:
    [
        {'field': ..., 'op': ..., 'value': ...}
    ]
    Especialmente, las condiciones OR internas se agrupan como lista de valores por campo.
    """
    import ast

    # Mapeo de nombres de clase de boto3 a nombres amigables
    OPERATOR_MAP = {
        "Eq": "Equals",
        "Equal": "Equals",
        "Equals": "Equals",
        "Gt": "GreaterThan",
        "Lt": "LessThan",
        "Between": "Between",
        "BeginsWith": "BeginsWith",
        "In": "In",
        "Le": "LessOrEqual",
        "Ge": "GreaterOrEqual",
        "And": "And",
        "Or": "Or",
        "Not": "Not"
        # Añadir más si es necesario
    }

    def to_friendly_op(op):
        return OPERATOR_MAP.get(op, op)

    def flatten_conditions(cond, op_context=None):
        '''
        Condición recursiva, retorna una lista de dicts [{field, op, value}]
        '''
        if hasattr(cond, "_values"):
            op_type = type(cond).__name__
            values = getattr(cond, "_values", [])

            if op_type.lower() == "and":
                result = []
                for sub in values:
                    result += flatten_conditions(sub, op_context="And")
                return result

            elif op_type.lower() == "or":
                group = {}
                rest  = []

                for sub in values:
                    parts = flatten_conditions(sub, op_context="Or")

                    if (len(parts) == 1 and 'field' in parts[0] and 'op' in parts[0]):
                        key = (parts[0]['field'], parts[0]['op'])
                        if key not in group:
                            group[key] = []
                        group[key].append(parts[0]['value'])
                    else:
                        rest.extend(parts)
                result = []
                for (field, op), vals in group.items():
                    if op == 'Between':
                        for v in vals:
                            result.append({'field': field, 'op': op, 'value': v})
                    else:
                        if len(vals) == 1:
                            result.append({'field': field, 'op': op, 'value': vals[0]})
                        else:
                            result.append({'field': field, 'op': 'Or', 'value': vals})
                result += rest
                return result

            elif op_type == "Not":
                return [{'op': 'Not', 'value': flatten_conditions(values[0])}]
            
            elif op_type == "Between" and len(values) == 3:
                key_obj = values[0]
                field = getattr(key_obj, 'name', str(key_obj))
                from_v = values[1]
                to_v = values[2]
                return [{'field': field, 'op': 'Between', 'value': [from_v, to_v]}]

            elif len(values) == 2:
                key_obj = values[0]
                field = getattr(key_obj, 'name', str(key_obj))
                op_friendly = to_friendly_op(op_type)
                val1 = values[1]

                if hasattr(val1, "_values"):
                    nested = flatten_conditions(val1)
                    if isinstance(nested, list):
                        return nested
                    return [nested]

                if isinstance(val1, str) and val1.startswith("["):
                    try:
                        val1 = ast.literal_eval(val1)
                    except Exception:
                        pass
                return [{'field': field, 'op': op_friendly, 'value': val1}]
            else:
                return [{'op': to_friendly_op(op_type), 'value': [str(v) for v in values]}]
        
        elif isinstance(cond, list):
            result = []
            for c in cond:
                result += flatten_conditions(c, op_context)
            return result
        # Valor simple
        return cond

    # Structure
    result = {}

    for k, v in qparams.items():
        if k == "FilterExpression":
            items = flatten_conditions(v)
            result[k] = items
        elif hasattr(v, '_values'):
            conds = flatten_conditions(v)
            result[k] = conds[0] if isinstance(conds, list) and len(conds) == 1 else conds
        elif hasattr(v, 'conditions') and isinstance(getattr(v, 'conditions', None), (list, tuple)):
            items = flatten_conditions(v)
            result[k] = items
        else:
            result[k] = {
                'type': type(v).__name__,
                'value': v
            }
    return result


def build_conditions(field, operator, conditions):
    if not conditions or len(conditions) < 2:
        raise ValueError(f"ddb_client.helpers | {operator} operator requires at least 2 conditions")

    result = conditions[0].build(field)
    for condition in conditions[1:]:
        if operator == 'or':
            result = result | condition.build(field)
        elif operator == 'and':
            result = result & condition.build(field)

    return result


def require_table(func):
    def wrapper(table, *args, **kwargs):
        if not table:
            logger.error("ddb_client | The parameter 'table' cannot be empty or None.")
            return None
        return func(table, *args, **kwargs)
    return wrapper


def normalize_allowed_fields(raw_allowed_fields: dict) -> dict:
    normalized = {}

    for table, fields in raw_allowed_fields.items():
        if isinstance(fields, (list, tuple, set)):
            normalized[table] = {field: (object,) for field in fields}
            continue

        if isinstance(fields, dict):
            normalized[table] = {
                field: tuple(types) if isinstance(types, (list, tuple, set)) else (types,)
                for field, types in fields.items()
            }
            continue

        raise TypeError(f'Invalid allowed_fields schema for table {table}')

    return normalized