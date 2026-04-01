import json
import logging

# declare a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def logger_runtime_event(event: dict):

    headers = event.get("headers", {})
    request_context = event.get("requestContext", {})
    authorizer_claims = request_context.get("authorizer", {}).get("jwt", {}).get("claims", {})
    stage_variables = event.get("stageVariables", {})

    event_source = event.get('event_source', "")
    records = event.get("Records", {})[0] if event_source=='aws:sqs' else {}

    details = event.get("detail", {})

    expected_body = [event.get("body", {}), records.get("body", {}), details.get("body", {})]
    body = next((b for b in expected_body if b), {})
    
    if isinstance(body, dict):
        body = json.dumps(body)

    APIGATEWAY = {
        "origin": headers.get("origin", ""),
        "device": headers.get("device-info", ""),
        "platform": headers.get("sec-ch-ua-platform", ""),
        "lambda_stage": stage_variables.get("LambdaAlias", ""),
        "api_stage": request_context.get("stage", ""),
        "raw_route": request_context.get("routeKey", ""),
        "domain": request_context.get("domainName", ""),
        "path": event.get("rawPath", ""),
        "query_string": event.get("queryStringParameters", ""),
        "path_parameters": event.get("pathParameters", ""),
        "user_id": authorizer_claims.get("id", ""),
        "user_email": authorizer_claims.get("email", ""),
        "user_role": authorizer_claims.get("role", ""),
        "business_id": authorizer_claims.get("business_id", ""),
        "business_role": authorizer_claims.get("business_role", ""),
    }


    SQS = {
        "attributes": records.get("attributes", {}),
        "messageAttributes": records.get("messageAttributes", {}),
    }



    EVENT_BRIDGE = {
        "eventBridgeTracer": details.get("EventBridgeTracer", ""),
        "origins": details.get("origin", {}),
        "source": event.get('source', 'Unknow'),
        "resource": event.get('resource', [])
    }
        
    log_data = {
        
        # ApiGateway
        **(APIGATEWAY if request_context.get('apiId') else {}),
        
        #sqs
        **(SQS if event_source == 'aws:sqs' else {}),
        
        #EventBridge
        
        **(EVENT_BRIDGE if details.get('EventBridgeTracer') else {}),
        
        # All
        "body": json.loads(body)
    }

    log_data = {k: v for k, v in log_data.items() if v}

    logger.info(json.dumps(log_data))


