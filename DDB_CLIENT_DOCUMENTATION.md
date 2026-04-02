# Documentación DDB Client y Utils

## Tabla de Contenidos
1. [Introducción](#introducción)
2. [Configuración](#configuración)
3. [Funciones de Búsqueda (Searches)](#funciones-de-búsqueda-searches)
4. [Funciones de Actualización (Updates)](#funciones-de-actualización-updates)
5. [Helpers y Comparadores](#helpers-y-comparadores)
6. [Utilidades (Utils)](#utilidades-utils)

---

## Introducción

El módulo `ddb_client` proporciona una interfaz completa para interactuar con DynamoDB de AWS. Incluye funciones para crear, leer, actualizar, eliminar y realizar operaciones en lote sobre tablas de DynamoDB.

---

## Configuración

### Estructura de config.py

El archivo `config.py` debe contener las siguientes configuraciones:

```python
# Número de páginas por consulta
pages = 100

# Habilitar paginación
paginate = True

# Campos permitidos por tabla (para operaciones de lectura)
fields = {
    'nombre_tabla': ['campo1', 'campo2', 'campo3.anidado'],
}

# Campos permitidos para crear (con tipos permitidos)
allowed_fields_to_create = {
    'nombre_tabla': {
        'campo1': (str,),
        'campo2': (int,),
        'campo3': (str, int),  # Acepta múltiples tipos
        'campo_dict': (dict,),
        'campo_lista': (list,),
    }
}

# Campos permitidos para actualizar (con tipos permitidos)
allowed_fields = {
    'nombre_tabla': {
        'campo1': (str,),
        'campo2': (int,),
        'campo3': (str, int),
    }
}

# Valores por defecto al crear registros
defaults = {
    'data': {
        'nombre_tabla': {
            'status': 'active',
            'version': 1,
            'campo_default': 'valor_default'
        }
    }
}
```

### Constantes Importantes

#### RESERVED_WORDS
Lista de palabras reservadas de DynamoDB que requieren manejo especial en las expresiones. El cliente las maneja automáticamente usando `ExpressionAttributeNames`.

---

## Funciones de Búsqueda (Searches)

### ddb_read

Busca y recupera items de una tabla DynamoDB.

#### Sintaxis
```python
ddb_read(table: str, params: dict, limit=config.paginate, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `params` | `dict` | Parámetros de búsqueda (claves y filtros) |
| `limit` | `bool` | Si se debe paginar o no (default: `config.paginate`) |

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `index` | `str` | Nombre del índice secundario a usar (GSI o LSI) |
| `order` | `str` | Orden de resultados: `'ASC'` o `'DESC'` (default: `'ASC'`) |
| `pages` | `int` | Número de items por página (default: `config.pages`) |
| `order_by` | `str` | Nombre del LSI para ordenar resultados |
| `log_errors` | `bool` | Registrar errores en logs (default: `True`) |
| `log_search` | `bool` | Registrar parámetros de búsqueda (default: `False`) |
| `log_qparams` | `bool` | Registrar query params construidos (default: `False`) |
| `log_schema` | `bool` | Registrar información del schema (default: `False`) |
| `fields` | `list` | Lista de campos específicos a retornar |

#### Estructura de params

```python
params = {
    # Claves requeridas
    'pk_field': 'valor_pk',  # Partition Key (requerido)
    'sk_field': 'valor_sk',  # Sort Key (opcional, según schema)
    
    # O usar operadores de comparación para SK
    'sk_field': DynamoComparison.BeginsWith('prefix', is_key=True),
    'sk_field': DynamoComparison.Between(start, end, is_key=True),
    
    # Filtros adicionales
    'campo1': 'valor',
    'campo2': DynamoComparison.Gt(100),
    'campo3': DynamoComparison.Contains('texto'),
    
    # Campos especiales
    'fields': ['campo1', 'campo2'],  # Proyección específica
    'LastEvaluatedKey': {...}  # Para paginación continuada
}
```

#### Retorna

```python
{
    'Items': [...],              # Lista de items encontrados
    'Count_Items': 10,           # Cantidad de items
    'LastEvaluatedKey': {...}    # Solo si hay más resultados (opcional)
}
```

#### Ejemplos

```python
# Búsqueda simple
result = ddb_read('usuarios', {
    'user_id': 'USER#123'
})

# Búsqueda con Sort Key y filtros
result = ddb_read('transacciones', {
    'user_id': 'USER#123',
    'timestamp': DynamoComparison.Between(start_date, end_date, is_key=True),
    'status': 'completed',
    'amount': DynamoComparison.Gt(100)
})

# Búsqueda usando índice secundario
result = ddb_read('usuarios', {
    'email': 'user@example.com'
}, index='email-index')

# Búsqueda con campos específicos
result = ddb_read('usuarios', {
    'user_id': 'USER#123',
    'fields': ['name', 'email', 'created_at']
})

# Búsqueda con paginación
result = ddb_read('items', {
    'category': 'electronics'
}, limit=True, pages=50)

# Continuar paginación
if 'LastEvaluatedKey' in result:
    next_page = ddb_read('items', {
        'category': 'electronics',
        'LastEvaluatedKey': result['LastEvaluatedKey']
    }, limit=True)
```

---

### batch_get_items

Recupera múltiples items en una sola operación batch (hasta 100 items por lote).

#### Sintaxis
```python
batch_get_items(table: str, source: list, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `source` | `list` | Lista de objetos que contienen las claves |

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `pk_name` | `str` | Ruta del campo PK en source (ej: `'user_id'` o `'data.user_id'`) |
| `sk_name` | `str` | Ruta del campo SK en source (si aplica) |
| `fields` | `list` | Campos específicos a retornar |
| `log_keys` | `bool` | Registrar claves construidas (default: `False`) |
| `log_result` | `bool` | Registrar resultado (default: `False`) |

#### Estructura de source

```python
source = [
    {'user_id': 'USER#1', 'timestamp': 12345},
    {'user_id': 'USER#2', 'timestamp': 67890},
    # ... hasta 100 items
]
```

#### Retorna

```python
[
    {item1},
    {item2},
    ...
]  # Lista de items recuperados
```

#### Ejemplos

```python
# Recuperar usuarios por ID
users = batch_get_items('usuarios', 
    source=[
        {'user_id': 'USER#1'},
        {'user_id': 'USER#2'},
        {'user_id': 'USER#3'}
    ],
    user_id='user_id'
)

# Con PK y SK
transactions = batch_get_items('transacciones',
    source=[
        {'user_id': 'USER#1', 'timestamp': 12345},
        {'user_id': 'USER#2', 'timestamp': 67890}
    ],
    user_id='user_id',
    timestamp='timestamp'
)

# Con campos anidados
items = batch_get_items('items',
    source=[
        {'data': {'item_id': 'ITEM#1'}},
        {'data': {'item_id': 'ITEM#2'}}
    ],
    item_id='data.item_id'
)

# Con proyección de campos específicos
users = batch_get_items('usuarios',
    source=[{'user_id': f'USER#{i}'} for i in range(1, 50)],
    user_id='user_id',
    fields=['name', 'email', 'status']
)
```

---

### ddb_counter

Cuenta el número total de items que cumplen con los criterios de búsqueda.

#### Sintaxis
```python
ddb_counter(table: str, params: dict, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `params` | `dict` | Parámetros de búsqueda (igual que ddb_read) |

#### Argumentos Opcionales (**args)

Acepta los mismos argumentos opcionales que `ddb_read`.

#### Retorna

```python
{
    'Count_Items': 150,  # Total de items encontrados
    'Pages': 3          # Número de páginas (basado en config.pages)
}
```

#### Ejemplos

```python
# Contar usuarios activos
result = ddb_counter('usuarios', {
    'tenant_id': 'TENANT#1',
    'status': 'active'
})
# result = {'Count_Items': 150, 'Pages': 2}

# Contar transacciones en un rango
result = ddb_counter('transacciones', {
    'user_id': 'USER#123',
    'timestamp': DynamoComparison.Between(start, end, is_key=True)
})

# Contar usando índice
result = ddb_counter('productos', {
    'category': 'electronics'
}, index='category-index')
```

---

## Funciones de Actualización (Updates)

### ddb_create

Crea un nuevo item en la tabla DynamoDB.

#### Sintaxis
```python
ddb_create(table: str, data: dict, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `data` | `dict` | Datos del item a crear |

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `log_qparams` | `bool` | Registrar item a crear (default: `True`) |
| `log_errors` | `bool` | Registrar errores (default: `True`) |

#### Campos Automáticos

La función agrega automáticamente:
- `id`: UUID v4 único
- `timestamp`: Timestamp en milisegundos
- `created_at`: Fecha ISO 8601
- Campos definidos en `config.defaults['data'][table]`

#### Retorna

```python
{
    'data': {...},     # Item creado completo
    'status': True     # True si se creó exitosamente
}
```

#### Ejemplos

```python
# Crear usuario
result = ddb_create('usuarios', {
    'user_id': 'USER#123',
    'name': 'Juan Pérez',
    'email': 'juan@example.com',
    'age': 30
})

# Crear con datos anidados
result = ddb_create('productos', {
    'product_id': 'PROD#456',
    'name': 'Laptop',
    'details': {
        'brand': 'Dell',
        'model': 'XPS 13'
    }
})

# El item creado incluirá:
# - id: "550e8400-e29b-41d4-a716-446655440000"
# - timestamp: 1648739200000
# - created_at: "2024-04-02T14:00:00.000Z"
# - Campos de config.defaults['data']['productos']
# - Todos los campos de data
```

---

### ddb_update

Actualiza un item existente en la tabla.

#### Sintaxis
```python
ddb_update(table: str, params: dict, data: dict, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `params` | `dict` | Claves del item a actualizar |
| `data` | `dict` | Campos a actualizar/eliminar |

#### Estructura de params

```python
params = {
    'pk_field': 'valor_pk',  # Requerido
    'sk_field': 'valor_sk',  # Requerido si la tabla tiene SK
    
    # Condiciones opcionales
    'campo_condicion': 'valor_esperado'
}
```

#### Estructura de data

```python
data = {
    'campo1': 'nuevo_valor',     # Actualizar campo
    'campo2': 123,               # Actualizar campo numérico
    'campo3.anidado': 'valor',   # Actualizar campo anidado
    'campo4': None               # Eliminar campo (REMOVE)
}
```

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `log_errors` | `bool` | Registrar errores (default: `True`) |
| `log_qparams` | `bool` | Registrar query construido (default: `False`) |
| `log_schema` | `bool` | Registrar schema (default: `False`) |
| `log_keys` | `bool` | Registrar keys construidas (default: `False`) |
| `log_criticals` | `bool` | Registrar campos filtrados (default: `True`) |

#### Retorna

```python
{
    'status': True,    # True si se actualizó exitosamente
    'row_count': 1     # Número de items actualizados (0 o 1)
}
```

#### Ejemplos

```python
# Actualizar usuario
result = ddb_update('usuarios',
    params={'user_id': 'USER#123'},
    data={'name': 'Juan Carlos', 'age': 31}
)

# Actualizar con Sort Key
result = ddb_update('transacciones',
    params={'user_id': 'USER#123', 'timestamp': 12345},
    data={'status': 'completed', 'notes': 'Procesado'}
)

# Eliminar campos (usando None)
result = ddb_update('usuarios',
    params={'user_id': 'USER#123'},
    data={'temp_token': None, 'session_id': None}
)

# Actualizar con condición
result = ddb_update('productos',
    params={
        'product_id': 'PROD#456',
        'version': 1  # Condición: solo actualizar si version == 1
    },
    data={'stock': 50, 'version': 2}
)

# Actualizar campos anidados
result = ddb_update('usuarios',
    params={'user_id': 'USER#123'},
    data={
        'settings.theme': 'dark',
        'settings.notifications': True
    }
)
```

---

### ddb_increase

Incrementa o decrementa valores numéricos de forma atómica.

#### Sintaxis
```python
ddb_increase(table: str, params: dict, data: dict, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `params` | `dict` | Claves del item y límites opcionales |
| `data` | `dict` | Campos a incrementar/decrementar |

#### Estructura de params

```python
params = {
    'pk_field': 'valor_pk',      # Requerido
    'sk_field': 'valor_sk',      # Requerido si aplica
    
    # Límites opcionales (por campo)
    'campo1_min': 0,             # Valor mínimo permitido
    'campo1_max': 1000,          # Valor máximo permitido
    'campo2_min': -100,
    'campo2_max': 500
}
```

#### Estructura de data

```python
data = {
    'campo1': 10,      # Incrementar en 10
    'campo2': -5,      # Decrementar en 5
    'campo3': 1        # Incrementar en 1
}
```

#### Comportamiento

- Si el campo no existe, se inicializa en 0 antes del incremento
- Los límites `_min` y `_max` previenen valores fuera de rango
- La operación falla si se exceden los límites definidos

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `log_errors` | `bool` | Registrar errores (default: `True`) |
| `log_qparams` | `bool` | Registrar query (default: `False`) |
| `log_schema` | `bool` | Registrar schema (default: `False`) |

#### Retorna

```python
{
    'status': True,    # True si se actualizó exitosamente
    'row_count': 1     # Número de items actualizados (0 o 1)
}
```

#### Ejemplos

```python
# Incrementar contador simple
result = ddb_increase('usuarios',
    params={'user_id': 'USER#123'},
    data={'login_count': 1}
)

# Incrementar con límite máximo
result = ddb_increase('usuarios',
    params={
        'user_id': 'USER#123',
        'credits_max': 1000  # No permite superar 1000
    },
    data={'credits': 50}
)

# Decrementar con límite mínimo
result = ddb_increase('inventario',
    params={
        'product_id': 'PROD#456',
        'stock_min': 0  # No permite valores negativos
    },
    data={'stock': -10}
)

# Múltiples campos con límites
result = ddb_increase('juego',
    params={
        'player_id': 'PLAYER#789',
        'lives_min': 0,
        'lives_max': 5,
        'score_min': 0
    },
    data={
        'lives': -1,
        'score': 100
    }
)
```

---

### ddb_delete

Elimina un item de la tabla.

#### Sintaxis
```python
ddb_delete(table: str, params: dict, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `params` | `dict` | Claves del item a eliminar |

#### Estructura de params

```python
params = {
    'pk_field': 'valor_pk',  # Requerido
    'sk_field': 'valor_sk'   # Requerido si la tabla tiene SK
}
```

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `log_errors` | `bool` | Registrar errores (default: `True`) |
| `return_values` | `bool` | Retornar item eliminado (default: `True`) |
| `log_qparams` | `bool` | Registrar query (default: `False`) |
| `log_schema` | `bool` | Registrar schema (default: `False`) |

#### Retorna

```python
{
    'status': True,    # True si se eliminó exitosamente
    'row_count': 1     # 1 si se eliminó, 0 si no existía
}
```

#### Ejemplos

```python
# Eliminar usuario
result = ddb_delete('usuarios', {
    'user_id': 'USER#123'
})

# Eliminar con Sort Key
result = ddb_delete('transacciones', {
    'user_id': 'USER#123',
    'timestamp': 12345
})

# Eliminar sin retornar valores (más eficiente)
result = ddb_delete('logs', {
    'log_id': 'LOG#456'
}, return_values=False)
```

---

### batch_delete_items

Elimina múltiples items en una operación batch (hasta 25 items por lote).

#### Sintaxis
```python
batch_delete_items(table: str, source: list, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `source` | `list` | Lista de objetos con las claves a eliminar |

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `pk_name` | `str` | Ruta del campo PK en source |
| `sk_name` | `str` | Ruta del campo SK en source (si aplica) |
| `log_keys` | `bool` | Registrar claves (default: `False`) |
| `log_result` | `bool` | Registrar resultado (default: `False`) |

#### Retorna

```python
{
    'status': True,     # True si se eliminó al menos un item
    'row_count': 25     # Número de items eliminados
}
```

#### Ejemplos

```python
# Eliminar múltiples usuarios
result = batch_delete_items('usuarios',
    source=[
        {'user_id': 'USER#1'},
        {'user_id': 'USER#2'},
        {'user_id': 'USER#3'}
    ],
    user_id='user_id'
)

# Eliminar con PK y SK
result = batch_delete_items('transacciones',
    source=[
        {'user_id': 'USER#1', 'timestamp': 12345},
        {'user_id': 'USER#1', 'timestamp': 67890},
        {'user_id': 'USER#2', 'timestamp': 11111}
    ],
    user_id='user_id',
    timestamp='timestamp'
)

# Eliminar con claves anidadas
result = batch_delete_items('items',
    source=[
        {'data': {'item_id': 'ITEM#1'}},
        {'data': {'item_id': 'ITEM#2'}}
    ],
    item_id='data.item_id'
)
```

---

### batch_create_items

Crea múltiples items en una operación batch (hasta 25 items por lote).

#### Sintaxis
```python
batch_create_items(table: str, source: list, **args)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `table` | `str` | Nombre de la tabla DynamoDB |
| `source` | `list` | Lista de objetos a crear |

#### Argumentos Opcionales (**args)

| Argumento | Tipo | Descripción |
|-----------|------|-------------|
| `pk_name` | `str` | Ruta del campo PK en source |
| `sk_name` | `str` | Ruta del campo SK en source (si aplica) |
| `log_keys` | `bool` | Registrar items (default: `False`) |
| `log_result` | `bool` | Registrar resultado (default: `False`) |

#### Campos Automáticos

Cada item creado incluye automáticamente:
- `id`: UUID v4 único
- `timestamp`: Timestamp en milisegundos
- `created_at`: Fecha ISO 8601
- Campos de `config.defaults['data'][table]`

#### Retorna

```python
{
    'status': True,     # True si se creó al menos un item
    'row_count': 25     # Número de items creados
}
```

#### Ejemplos

```python
# Crear múltiples usuarios
result = batch_create_items('usuarios',
    source=[
        {'user_id': 'USER#1', 'name': 'Juan', 'email': 'juan@example.com'},
        {'user_id': 'USER#2', 'name': 'María', 'email': 'maria@example.com'},
        {'user_id': 'USER#3', 'name': 'Pedro', 'email': 'pedro@example.com'}
    ],
    user_id='user_id'
)

# Crear con PK y SK
result = batch_create_items('transacciones',
    source=[
        {'user_id': 'USER#1', 'timestamp': 12345, 'amount': 100},
        {'user_id': 'USER#1', 'timestamp': 67890, 'amount': 200}
    ],
    user_id='user_id',
    timestamp='timestamp'
)

# Crear desde datos anidados
result = batch_create_items('productos',
    source=[
        {'info': {'product_id': 'PROD#1'}, 'name': 'Laptop'},
        {'info': {'product_id': 'PROD#2'}, 'name': 'Mouse'}
    ],
    product_id='info.product_id'
)
```

---

## Helpers y Comparadores

### DynamoComparison

Clase que proporciona operadores de comparación para búsquedas y filtros en DynamoDB.

#### Operadores de Comparación

##### Eq - Igual a
```python
DynamoComparison.Eq(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'status': DynamoComparison.Eq('active')
}
```

##### Ne - Diferente de
```python
DynamoComparison.Ne(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'status': DynamoComparison.Ne('deleted')
}
```

##### Lt - Menor que
```python
DynamoComparison.Lt(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'age': DynamoComparison.Lt(30),
    'timestamp': DynamoComparison.Lt(timestamp_max, is_key=True)
}
```

##### Le - Menor o igual que
```python
DynamoComparison.Le(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'product_id': 'PROD#123',
    'stock': DynamoComparison.Le(10)  # Stock <= 10
}
```

##### Gt - Mayor que
```python
DynamoComparison.Gt(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'score': DynamoComparison.Gt(100)
}
```

##### Ge - Mayor o igual que
```python
DynamoComparison.Ge(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'age': DynamoComparison.Ge(18)  # Edad >= 18
}
```

##### Between - Entre dos valores
```python
DynamoComparison.Between(start, end, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'timestamp': DynamoComparison.Between(start_date, end_date, is_key=True),
    'age': DynamoComparison.Between(18, 65)
}
```

##### BeginsWith - Comienza con
```python
DynamoComparison.BeginsWith(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'tenant_id': 'TENANT#1',
    'user_id': DynamoComparison.BeginsWith('USER#', is_key=True)
}
```

##### Contains - Contiene
```python
DynamoComparison.Contains(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'tags': DynamoComparison.Contains('premium')
}
```

##### NotContains - No contiene
```python
DynamoComparison.NotContains(value, is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'tags': DynamoComparison.NotContains('banned')
}
```

##### In - En una lista
```python
DynamoComparison.In(values, is_key=False)
```
**Ejemplo:**
```python
params = {
    'tenant_id': 'TENANT#1',
    'status': DynamoComparison.In(['active', 'pending', 'trial'])
}
```

##### NotNull - Existe
```python
DynamoComparison.NotNull(is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'phone': DynamoComparison.NotNull()  # El campo phone existe
}
```

##### Null - No existe
```python
DynamoComparison.Null(is_key=False)
```
**Ejemplo:**
```python
params = {
    'user_id': 'USER#123',
    'deleted_at': DynamoComparison.Null()  # El campo deleted_at no existe
}
```

#### Operadores Lógicos

##### Or - Operador OR
```python
DynamoComparison.Or([condition1, condition2, ...])
```
**Ejemplo:**
```python
params = {
    'tenant_id': 'TENANT#1',
    'status': DynamoComparison.Or([
        DynamoComparison.Eq('active'),
        DynamoComparison.Eq('trial'),
        DynamoComparison.Eq('pending')
    ])
}
```

##### And - Operador AND
```python
DynamoComparison.And([condition1, condition2, ...])
```
**Ejemplo:**
```python
params = {
    'tenant_id': 'TENANT#1',
    'age': DynamoComparison.And([
        DynamoComparison.Ge(18),
        DynamoComparison.Le(65)
    ])
}
```

#### Parámetro is_key

- `is_key=True`: Usar para Sort Keys en `KeyConditionExpression`
- `is_key=False`: Usar para filtros en `FilterExpression` (default)

**Ejemplo completo:**
```python
result = ddb_read('transacciones', {
    'user_id': 'USER#123',
    'timestamp': DynamoComparison.Between(start, end, is_key=True),  # Sort Key
    'amount': DynamoComparison.Gt(100),  # Filter
    'status': DynamoComparison.In(['completed', 'pending'])  # Filter
})
```

---

### convert_decimals

Convierte objetos Decimal de DynamoDB a otros tipos.

#### Sintaxis
```python
convert_decimals(obj, convertion_type=str)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `obj` | `any` | Objeto a convertir (dict, list o Decimal) |
| `convertion_type` | `type` | Tipo destino (default: `str`) |

#### Ejemplos

```python
from ddb_client.helpers import convert_decimals

# De DynamoDB
result = ddb_read('productos', {'product_id': 'PROD#123'})
item = result['Items'][0]
# item = {'price': Decimal('19.99'), 'stock': Decimal('50')}

# Convertir a string
clean_item = convert_decimals(item)
# clean_item = {'price': '19.99', 'stock': '50'}

# Convertir a float
clean_item = convert_decimals(item, float)
# clean_item = {'price': 19.99, 'stock': 50.0}

# Convertir a int
clean_item = convert_decimals(item, int)
# clean_item = {'price': 19, 'stock': 50}

# Con listas anidadas
items = convert_decimals(result['Items'], float)
```

---

## Utilidades (Utils)

### describe_schema

Obtiene el schema completo de una tabla DynamoDB incluyendo índices.

#### Sintaxis
```python
describe_schema(table: str)
```

#### Retorna

```python
{
    'nombre_tabla': {
        'pk': 'campo_pk',
        'pk_type': 'S',  # 'S' (String), 'N' (Number), 'B' (Binary)
        'sk': 'campo_sk',
        'sk_type': 'N',
        'attributes_definitions': {
            'campo_pk': 'S',
            'campo_sk': 'N',
            'otros_campos_indexados': 'S'
        },
        'nombre_gsi': {
            'pk': 'campo_gsi_pk',
            'pk_type': 'S',
            'sk': 'campo_gsi_sk',
            'sk_type': 'N'
        },
        'LSI': ['nombre_lsi_1', 'nombre_lsi_2']
    }
}
```

#### Ejemplo

```python
from ddb_client.utils import describe_schema

schema = describe_schema('usuarios')
print(schema)
# {
#     'usuarios': {
#         'pk': 'user_id',
#         'pk_type': 'S',
#         'sk': 'timestamp',
#         'sk_type': 'N',
#         'attributes_definitions': {...},
#         'email-index': {
#             'pk': 'email',
#             'pk_type': 'S',
#             'sk': 'timestamp',
#             'sk_type': 'N'
#         },
#         'LSI': ['created_at-index']
#     }
# }
```

---

### show_schema_details

Formatea el schema de una tabla en un formato legible.

#### Sintaxis
```python
show_schema_details(table_name: str, schema: dict)
```

#### Retorna

```python
{
    'pk': 'user_id (S)',
    'sk': 'timestamp (N)',
    'email-index': 'GSI',
    'LSI': ['created_at-index']
}
```

---

### convert_to_field_type

Convierte un valor al tipo correspondiente de DynamoDB.

#### Sintaxis
```python
convert_to_field_type(value, dynamo_type)
```

#### Parámetros

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `value` | `any` | Valor a convertir |
| `dynamo_type` | `str` | Tipo DynamoDB: `'S'`, `'N'`, `'B'` |

#### Ejemplos

```python
from ddb_client.utils import convert_to_field_type

# Convertir a String
value = convert_to_field_type(123, 'S')  # '123'

# Convertir a Number
value = convert_to_field_type('456', 'N')  # 456

# Convertir a Binary
value = convert_to_field_type('data', 'B')  # b'data'
```

---

### get_nested_value

Obtiene un valor anidado de un diccionario.

#### Sintaxis
```python
get_nested_value(data: dict, keys: list)
```

#### Ejemplos

```python
from ddb_client.utils import get_nested_value

data = {
    'user': {
        'profile': {
            'name': 'Juan'
        }
    }
}

name = get_nested_value(data, ['user', 'profile', 'name'])
# name = 'Juan'

# Si la clave no existe
value = get_nested_value(data, ['user', 'address', 'city'])
# value = None
```

---

### extract_keys

Extrae las claves PK y SK de un diccionario de argumentos.

#### Sintaxis
```python
extract_keys(keys_fields: dict)
```

#### Retorna

```python
(pk_name, pk_value, sk_name, sk_value)
```

#### Ejemplo

```python
from ddb_client.utils import extract_keys

args = {'user_id': 'users.id', 'timestamp': 'created_at'}
pk_name, pk_value, sk_name, sk_value = extract_keys(args)
# pk_name = 'user_id'
# pk_value = 'users.id'
# sk_name = 'timestamp'
# sk_value = 'created_at'
```

---

### flatten_dict

Aplana un diccionario anidado en un diccionario de un solo nivel.

#### Sintaxis
```python
flatten_dict(data: dict, parent_key='')
```

#### Ejemplos

```python
from ddb_client.utils import flatten_dict

nested = {
    'user': {
        'profile': {
            'name': 'Juan',
            'age': 30
        },
        'email': 'juan@example.com'
    }
}

flat = flatten_dict(nested)
# {
#     'user.profile.name': 'Juan',
#     'user.profile.age': 30,
#     'user.email': 'juan@example.com'
# }
```

---

### chunk

Divide una lista en sublistas de tamaño específico.

#### Sintaxis
```python
chunk(iterable, size)
```

#### Ejemplos

```python
from ddb_client.utils import chunk

items = list(range(1, 101))  # [1, 2, 3, ..., 100]

for batch in chunk(items, 25):
    print(len(batch))  # 25, 25, 25, 25

# Uso en operaciones batch
for batch in chunk(items, 25):
    batch_delete_items('tabla', batch, ...)
```

---

### buildProjectionExpression

Construye una ProjectionExpression para DynamoDB.

#### Sintaxis
```python
buildProjectionExpression(select: list)
```

#### Retorna

```python
{
    'ProjectionExpression': 'field1,field2,#reserved',
    'ExpressionAttributeNames': {'#reserved': 'reserved'}
}
```

#### Ejemplo

```python
from ddb_client.utils import buildProjectionExpression

fields = ['name', 'email', 'status', 'data.nested']
projection = buildProjectionExpression(fields)
# {
#     'ProjectionExpression': 'name,email,#status,data.nested',
#     'ExpressionAttributeNames': {'#status': 'status'}
# }
```

---

### extract_search_structure

Convierte los parámetros de query en una estructura legible para debug.

#### Sintaxis
```python
extract_search_structure(qparams: dict)
```

#### Ejemplo

```python
from ddb_client.utils import extract_search_structure

# Interno en ddb_read con log_qparams=True
result = ddb_read('usuarios', {
    'user_id': 'USER#123',
    'age': DynamoComparison.Gt(18)
}, log_qparams=True)

# Registrará una estructura como:
# {
#     'KeyConditionExpression': {'field': 'user_id', 'op': 'Equals', 'value': 'USER#123'},
#     'FilterExpression': [{'field': 'age', 'op': 'GreaterThan', 'value': 18}]
# }
```

---

### build_conditions

Construye condiciones lógicas OR/AND para filtros.

#### Sintaxis
```python
build_conditions(field: str, operator: str, conditions: list)
```

#### Ejemplo (uso interno)

```python
from ddb_client.helpers import DynamoComparison

# Esto usa build_conditions internamente
params = {
    'user_id': 'USER#123',
    'status': DynamoComparison.Or([
        DynamoComparison.Eq('active'),
        DynamoComparison.Eq('trial')
    ])
}
```

---

### require_table

Decorador que valida que el parámetro `table` no esté vacío.

#### Ejemplo (uso interno)

```python
from ddb_client.utils import require_table

@require_table
def mi_funcion(table: str, params: dict):
    # La función solo se ejecuta si table no es None o ''
    pass

# Si table es None o '', registra error y retorna None
```

---

### normalize_allowed_fields

Normaliza la configuración de `allowed_fields` a un formato consistente.

#### Sintaxis
```python
normalize_allowed_fields(raw_allowed_fields: dict)
```

#### Ejemplo (uso interno)

```python
from ddb_client.utils import normalize_allowed_fields

# Formato simple (lista)
raw = {
    'usuarios': ['name', 'email', 'age']
}

normalized = normalize_allowed_fields(raw)
# {
#     'usuarios': {
#         'name': (object,),
#         'email': (object,),
#         'age': (object,)
#     }
# }

# Formato con tipos
raw = {
    'usuarios': {
        'name': str,
        'age': int,
        'tags': (list, str)
    }
}

normalized = normalize_allowed_fields(raw)
# {
#     'usuarios': {
#         'name': (str,),
#         'age': (int,),
#         'tags': (list, str)
#     }
# }
```

---

## Ejemplos Avanzados

### Búsqueda con Múltiples Filtros y Paginación

```python
# Primera página
result = ddb_read('transacciones', {
    'user_id': 'USER#123',
    'timestamp': DynamoComparison.Between(start_date, end_date, is_key=True),
    'amount': DynamoComparison.Gt(100),
    'status': DynamoComparison.In(['completed', 'pending']),
    'category': DynamoComparison.BeginsWith('ELECTRONICS')
}, limit=True, pages=50, order='DESC')

# Páginas siguientes
while 'LastEvaluatedKey' in result:
    result = ddb_read('transacciones', {
        'user_id': 'USER#123',
        'timestamp': DynamoComparison.Between(start_date, end_date, is_key=True),
        'amount': DynamoComparison.Gt(100),
        'status': DynamoComparison.In(['completed', 'pending']),
        'LastEvaluatedKey': result['LastEvaluatedKey']
    }, limit=True, pages=50)
    
    # Procesar items
    for item in result['Items']:
        process_item(item)
```

### Operación Batch Completa

```python
# Obtener IDs desde otra fuente
users_to_sync = [
    {'external_id': 'EXT#1'},
    {'external_id': 'EXT#2'},
    # ... hasta 100
]

# Leer datos actuales
current_data = batch_get_items('usuarios',
    source=users_to_sync,
    user_id='external_id',
    fields=['user_id', 'status', 'last_sync']
)

# Actualizar con lógica personalizada
updates = []
for user in current_data:
    if user['status'] == 'active':
        ddb_update('usuarios',
            params={'user_id': user['user_id']},
            data={'last_sync': datetime.now().isoformat()}
        )

# Crear nuevos registros
new_users = [u for u in users_to_sync if u['external_id'] not in [c['user_id'] for c in current_data]]
if new_users:
    batch_create_items('usuarios',
        source=new_users,
        user_id='external_id'
    )
```

### Actualización Condicional con Reintentos

```python
def update_with_version_control(user_id, new_data, max_retries=3):
    for attempt in range(max_retries):
        # Leer versión actual
        result = ddb_read('usuarios', {'user_id': user_id})
        if not result['Items']:
            return {'status': False, 'error': 'User not found'}
        
        user = result['Items'][0]
        current_version = user.get('version', 0)
        
        # Intentar actualizar con condición de versión
        update_result = ddb_update('usuarios',
            params={
                'user_id': user_id,
                'version': current_version  # Condición
            },
            data={
                **new_data,
                'version': current_version + 1
            }
        )
        
        if update_result['status']:
            return update_result
        
        # Si falla, esperar y reintentar
        time.sleep(0.1 * (2 ** attempt))
    
    return {'status': False, 'error': 'Max retries exceeded'}
```

### Búsqueda Multi-Índice

```python
# Buscar por email usando GSI
by_email = ddb_read('usuarios', {
    'email': 'user@example.com'
}, index='email-index')

# Buscar por tenant y filtrar
by_tenant = ddb_read('usuarios', {
    'tenant_id': 'TENANT#1',
    'status': DynamoComparison.Eq('active'),
    'created_at': DynamoComparison.Between(start, end, is_key=True)
}, index='tenant-index', order_by='created_at-lsi', order='DESC')

# Combinar resultados
all_users = by_email['Items'] + by_tenant['Items']
unique_users = {u['user_id']: u for u in all_users}.values()
```

---

## Mejores Prácticas

### 1. Uso de allowed_fields

Siempre define `allowed_fields` y `allowed_fields_to_create` en config.py para:
- Prevenir modificación de campos no autorizados
- Validar tipos de datos
- Documentar el schema esperado

### 2. Manejo de Errores

```python
result = ddb_update('usuarios',
    params={'user_id': user_id},
    data={'status': 'active'},
    log_errors=True  # Registrar errores para debugging
)

if not result['status']:
    # Manejar error
    logger.error(f'Failed to update user {user_id}')
```

### 3. Paginación Eficiente

Para grandes volúmenes de datos:
- Usa `limit=True` para paginar
- Ajusta `pages` según tu caso de uso
- Procesa items por lotes

### 4. Proyección de Campos

Para optimizar costos y performance:
```python
result = ddb_read('usuarios', {
    'user_id': 'USER#123',
    'fields': ['name', 'email']  # Solo campos necesarios
})
```

### 5. Operaciones Batch

Para múltiples items, siempre usa batch:
- `batch_get_items` en lugar de múltiples `ddb_read`
- `batch_create_items` en lugar de múltiples `ddb_create`
- `batch_delete_items` en lugar de múltiples `ddb_delete`

### 6. Incrementos Atómicos

Usa `ddb_increase` con límites para contadores:
```python
ddb_increase('usuarios',
    params={
        'user_id': 'USER#123',
        'credits_min': 0,
        'credits_max': 10000
    },
    data={'credits': 50}
)
```

### 7. Actualización de Campos Anidados

Para modificar solo partes de un objeto anidado:
```python
ddb_update('usuarios',
    params={'user_id': 'USER#123'},
    data={
        'settings.theme': 'dark',  # Solo actualiza theme
        'settings.language': 'es'  # Solo actualiza language
    }
)
```

---

## Solución de Problemas

### Error: "Key mismatch error"

**Causa:** Las claves proporcionadas no coinciden con el schema de la tabla.

**Solución:**
```python
# Verificar schema
from ddb_client.utils import describe_schema, show_schema_details

schema = describe_schema('mi_tabla')
details = show_schema_details('mi_tabla', schema)
print(details)
```

### Error: "Schema not declared in allowed_fields"

**Causa:** La tabla no está configurada en config.py.

**Solución:** Agregar configuración:
```python
# En config.py
allowed_fields = {
    'mi_tabla': {
        'campo1': (str,),
        'campo2': (int,),
    }
}
```

### Error: "ConditionalCheckFailedException"

**Causa:** La condición de actualización falló (ej: límites de increment).

**Solución:** Verificar límites y condiciones:
```python
# Con logging para debug
ddb_increase('tabla',
    params={'id': '123', 'counter_max': 100},
    data={'counter': 10},
    log_qparams=True  # Ver query construido
)
```

### Items no encontrados en búsqueda

**Solución:** Habilitar logs para debug:
```python
result = ddb_read('tabla', {
    'pk': 'valor'
}, log_schema=True, log_qparams=True, log_search=True)
```

---

## Changelog y Versiones

### Características Principales

- ✅ CRUD completo (Create, Read, Update, Delete)
- ✅ Operaciones batch optimizadas
- ✅ Soporte para GSI y LSI
- ✅ Comparadores avanzados (Between, Contains, etc.)
- ✅ Paginación automática
- ✅ Validación de tipos
- ✅ Manejo de campos anidados
- ✅ Incrementos atómicos con límites
- ✅ Manejo automático de palabras reservadas
- ✅ Logging configurable

---

## Contacto y Soporte

Para reportar bugs o solicitar features, contacta al equipo de desarrollo.

---

**Última actualización:** 2024-04-02
