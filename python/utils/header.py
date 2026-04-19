import json

from utils.logs import build_error

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

def validate_user(user: dict, allowed_roles: dict, role_validation: bool=None, params: dict={}):
    if user['status'] in ('DELETED', 'BANNED'):
        return {
            "status": False,
            "body": build_error(
                status_code=403,
                log_message=f'[ AUTH ] The user [ {user["id"]} - {user["status"]} ] has no authorization to perform this function.',
                result_message='UnauthorizedException',
                error_message=f'The user [ {user["id"]} ] with the status [ {user["status"]} ] has no authorization to perform this function.'
            )
        }
        
    if role_validation:
        is_admin = user['role'] == 'admin'
        is_owner = user['id'] == params.get('id', params.get('user_id'))

        if user['role'] not in allowed_roles or (not is_admin and not is_owner):
            return { 
                "status": False,
                "body": build_error(
                    status_code= 403,
                    log_message= f'[ AUTH ] The user [ {user['id']} - {user['role']} ]',
                    result_message= 'UnauthorizedException',
                    error_message= f'The user [ {user['id']} ] with the role [ {user['role']} ] has no authorization to perform this function.'
                )
            }
            
        return {'status': True, 'is_admin': is_admin, 'is_owner': is_owner}
    
    if user['role'] not in allowed_roles:
        return { 
            "status": False,
            "body": build_error(
                status_code=403,
                log_message=f'[ AUTH ] The user [ {user['id']} - {user['role']} ] has no authorization to perform this function.',
                result_message='UnauthorizedException',
                error_message=f'The user [ {user['id']} ] with the role [ {user['role']} ] has no authorization to perform this function.'
            )
        }
    
    return {"status": True}