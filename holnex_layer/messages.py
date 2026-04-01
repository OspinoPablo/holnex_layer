
DeployedProductionException = \
"""
    ❌ ERROR: You cannot deploy to production from the `{current_branch}` branch. Please switch to `master` first.
"""

MessageMissingException = \
"""
    ❌ ERROR: You must provide a deployment message with `--message 'Your message here'` for production.
"""

EnvironmentDefineException = \
"""
    ❌ ERROR: You must define the development environment to which you want to release the version. Use --env 'develop' or --env 'production'
"""

PublishException = \
"""
    ❌ ERROR publishing Layer: {error_message}
"""

SuccessfullLaunched = \
"""
    ✅ The version was successfully launched {layer_name} v{layer_version}
"""

FileEmptyException = \
"""
    ❌ ERROR: The ZIP file is empty. Please check the contents of the 'python/' folder.
"""

UnauthorizerException = \
"""
    ❌ ERROR: No AWS credentials found. Please set your credentials with `aws configure`.
"""

UsageExamples = \
"""
    Ejemplos de uso:
    
    # Listar todas las versiones
    python3 cleanup_layers.py --env develop --list
    
    # Eliminar versiones específicas
    python3 cleanup_layers.py --env develop --versions "1,2,3"
    
    # Eliminar un rango de versiones
    python3 cleanup_layers.py --env develop --versions "1-10"
    
    # Combinar rangos y versiones individuales
    python3 cleanup_layers.py --env develop --versions "1-5,8,10-15"
    
    # Modo dry-run (simular sin eliminar)
    python3 cleanup_layers.py --env develop --versions "1-10" --dry-run
    
    # Forzar eliminación sin confirmación
    python3 cleanup_layers.py --env production --versions "1-5" --force
"""