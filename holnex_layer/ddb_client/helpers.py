from boto3.dynamodb.conditions import Key, Attr
from ddb_client.utils import convert_to_field_type, build_conditions
from decimal import Decimal

class DynamoComparison:
    def __init__(self, operator, value=None, start=None, end=None, is_key=False, conditions=None):
        self.operator = operator
        self.value = value
        self.start = start
        self.end = end
        self.is_key = is_key
        self.conditions = conditions  # Para operadores lógicos OR/AND

    def __repr__(self):
        if self.operator in ['or', 'and']:
            return f"<DynamoComparison op={self.operator}, conditions={len(self.conditions) if self.conditions else 0}>"
        if self.operator == 'between':
            return f"<DynamoComparison op={self.operator}, start={self.start}, end={self.end}, is_key={self.is_key}>"
            
        return f"<DynamoComparison op={self.operator}, value={self.value}, is_key={self.is_key}>"

    def build(self, field, _type=None):

        if self.value is not None and _type is None:
            _type = 'N' if isinstance(self.value, int) else 'S'
        

        # Seleccionar Key o Attr según is_key
        field_builder = Key if self.is_key else Attr

        if self.conditions:
            self.value = convert_to_field_type(self.value, _type)

            if self.start and self.end:
                self.start = convert_to_field_type(self.start, _type)
                self.end = convert_to_field_type(self.end, _type)

        
        if self.operator == 'or':
            return build_conditions(field, 'or', self.conditions)
        elif self.operator == 'and':
            return build_conditions(field, 'and', self.conditions)
        elif self.operator == 'eq':
            return field_builder(field).eq(self.value)
        elif self.operator == 'ne':
            return field_builder(field).ne(self.value)
        elif self.operator == 'le':
            return field_builder(field).lte(self.value)
        elif self.operator == 'lt':
            return field_builder(field).lt(self.value)
        elif self.operator == 'ge':
            return field_builder(field).gte(self.value)
        elif self.operator == 'gt':
            return field_builder(field).gt(self.value)
        elif self.operator == 'not_null':
            return field_builder(field).exists()
        elif self.operator == 'null':
            return field_builder(field).not_exists()
        elif self.operator == 'contains':
            return field_builder(field).contains(self.value)
        elif self.operator == 'not_contains':
            return field_builder(field).not_contains(self.value)
        elif self.operator == 'begins_with':
            return field_builder(field).begins_with(self.value)
        elif self.operator == 'in':
            return field_builder(field).is_in(self.value)
        elif self.operator == 'between':
            return field_builder(field).between(self.start, self.end)
        else:
            raise ValueError(f"ddb_client.helpers | Unknown operator: {self.operator}")
    
    # Igual a
    @staticmethod
    def Eq(value, is_key=False):
        return DynamoComparison('eq', value=value, is_key=is_key)
    
    # Diferente a
    @staticmethod
    def Ne(value, is_key=False):
        return DynamoComparison('ne', value=value, is_key=is_key)
    
    # Menor o Igual a
    @staticmethod
    def Le(value, is_key=False):
        return DynamoComparison('le', value=value, is_key=is_key)
    
    # Menor que (Lt)
    @staticmethod
    def Lt(value, is_key=False):
        return DynamoComparison('lt', value=value, is_key=is_key)
    
    # Mayor o Igual a (Ge)
    @staticmethod
    def Ge(value, is_key=False):
        return DynamoComparison('ge', value=value, is_key=is_key)
    
    # Mayor que (Gt)
    @staticmethod
    def Gt(value, is_key=False):
        return DynamoComparison('gt', value=value, is_key=is_key)
    
    #Existe (NotNull)
    @staticmethod
    def NotNull(is_key=False):
        return DynamoComparison('not_null', is_key=is_key)
    
    # No Existe (Null)
    @staticmethod
    def Null(is_key=False):
        return DynamoComparison('null', is_key=is_key)
    
    # Contiene (Contains)
    @staticmethod
    def Contains(value, is_key=False):
        return DynamoComparison('contains', value=value, is_key=is_key)
    
    # No Contiene (NotContains)
    @staticmethod
    def NotContains(value, is_key=False):
        return DynamoComparison('not_contains', value=value, is_key=is_key)
    
    # Empieza con (BeginsWith)
    @staticmethod
    def BeginsWith(value, is_key=False):
        return DynamoComparison('begins_with', value=value, is_key=is_key)
    
    # En una Lista (In)
    @staticmethod
    def In(values, is_key=False):
        return DynamoComparison('in', value=values, is_key=is_key)
    
    # Entre Dos Valores (Between)
    @staticmethod
    def Between(start, end, is_key=False):
        return DynamoComparison('between', start=start, end=end, is_key=is_key)
    
    # Operador Lógico OR
    @staticmethod
    def Or(conditions):
        return DynamoComparison('or', conditions=conditions)
    
    # Operador Lógico AND
    @staticmethod
    def And(conditions):
        return DynamoComparison('and', conditions=conditions)


def convert_decimals(obj, convertion_type=str):
    if isinstance(obj, dict):
        return {k: convert_decimals(v, convertion_type) for k, v in obj.items()}
        
    elif isinstance(obj, list):
        return [convert_decimals(i, convertion_type) for i in obj]
        
    elif isinstance(obj, Decimal):
        return convertion_type(obj)
        
    else:
        return obj

