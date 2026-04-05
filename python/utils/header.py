import json

def params_parser(value):
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value
    

def header_management(event: dict, m_keys=[], m_data=[], id_param_name=None):
    params = {}
    data   = {}
    body   = event.get('body', {})
    
    if body:
        data = json.loads(body)['data'] if isinstance(body, str) else body['data']
    
    # Parameters management
    if 'pathParameters' in event:
        p_params = event['pathParameters']
        params.update({k: params_parser(v) for k, v in p_params.items()})

    if 'queryStringParameters' in event:
        q_params = event['queryStringParameters']
        params.update({k: params_parser(v) for k, v in q_params.items()})

    if id_param_name in params:
        params['id'] = params.pop(id_param_name)

    missing_keys = [ f'param.{k}' for k in m_keys if k not in params ]
    missing_data = [ f'data.{k}' for k in m_data if k not in data ]

    if missing_keys:
        raise KeyError(missing_keys)

    if missing_data:
        raise KeyError(missing_data)
        
    return params, data