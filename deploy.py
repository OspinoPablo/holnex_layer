import os
import subprocess
import boto3
import zipfile
import argparse
import botocore.exceptions
from messages import *

# Configuración
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAYER_SOURCE_DIR = os.path.join(BASE_DIR, "python")
ZIP_FILE = "holnex_client_layer.zip"
LAYER_NAME_PROD = "HolnexLayer"
LAYER_NAME_DEV = "HolnexLayerDevelop"
COMPATIBLE_RUNTIMES = ["python3.12", "python3.13", "python3.14"]
COMPATIBLE_ARCHITECTURES = ["x86_64", "arm64"]

# Configurar argumentos
parser = argparse.ArgumentParser(description="Deploy Lambda Layer")
parser.add_argument("--env", type=str, help="Deploy to enviroment layer")
parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
parser.add_argument("--message", type=str, help="Deployment message (required for production)")
args = parser.parse_args()

# Obtener la rama actual de Git
def get_git_branch():
    try:
        branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode("utf-8").strip()
        return branch
    except subprocess.CalledProcessError:
        print("❌ ERROR: No se pudo determinar la rama de Git. ¿Estás en un repositorio válido?")
        exit(1)

current_branch = get_git_branch()

# Determinar el entorno a usar
if args.env == 'develop':
    layer_name = LAYER_NAME_DEV
    env_mode = "Development"


elif args.env == 'production':
    
    if current_branch != "master":
        print(DeployedProductionException.format(current_branch=current_branch))
        exit(1)

    if not args.message:
        print(MessageMissingException)
        exit(1)

    layer_name = LAYER_NAME_PROD
    env_mode = "Production"


else:
    print(EnvironmentDefineException)
    exit(1)

print(f"🚀 {env_mode} mode actived")

# Verificar si las credenciales de AWS están configuradas
try:
    boto3.client("sts").get_caller_identity()
except botocore.exceptions.NoCredentialsError:
    print(UnauthorizerException)
    exit(1)

print(" ------ ")
print("📦 Compressing files from the layer")
if not os.path.isdir(LAYER_SOURCE_DIR):
    print(f"❌ ERROR: Source directory not found: {LAYER_SOURCE_DIR}")
    exit(1)

zip_file_path = os.path.join(BASE_DIR, ZIP_FILE)
with zipfile.ZipFile(ZIP_FILE, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(LAYER_SOURCE_DIR):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.relpath(file_path, BASE_DIR)  # Mantiene la carpeta python/ en el ZIP
            zipf.write(file_path, arcname)
            if args.verbose or env_mode=='Production':
                print(f"   ➜ Adding to ZIP: {file_path}")

# Verificar si el archivo ZIP tiene contenido
with zipfile.ZipFile(zip_file_path, 'r') as zipf:
    if not zipf.namelist():
        print(FileEmptyException)
        exit(1)

if os.path.getsize(zip_file_path) == 0:
    print(FileEmptyException)
    exit(1)

# Leer el contenido del archivo ZIP en memoria
with open(zip_file_path, "rb") as f:
    zip_bytes = f.read()

# Publicar nueva versión de la Layer en AWS Lambda
print(f"📡 Publishing...")
lambda_client = boto3.client("lambda")

try:
    response = lambda_client.publish_layer_version(
        LayerName=layer_name,
        Content={'ZipFile': zip_bytes},  # Se sube el ZIP directamente
        CompatibleRuntimes=COMPATIBLE_RUNTIMES,
        CompatibleArchitectures=COMPATIBLE_ARCHITECTURES,
        Description=args.message if args.message else "-"
    )

    layer_version = response["Version"]
    print(SuccessfullLaunched.format(layer_name=layer_name, layer_version=layer_version))

except botocore.exceptions.ClientError as error:
    print(PublishException.format(error_message=error.response['Error']['Message']))
    exit(1)

print("🎉 ¡Update completed!")
