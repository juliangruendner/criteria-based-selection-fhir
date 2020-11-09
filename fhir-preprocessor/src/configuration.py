import os

HOSTEXTERN = os.environ.get("HOSTEXTERN", "localhost")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "refInt_preproc_db")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "preprocessor")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "preprocessor")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "preprocessor")
FHIR_POSTGRES_HOST = os.environ.get("FHIR_POSTGRES_HOST", "fhir-gw_fhir-db_1")
FHIR_POSTGRES_DB = os.environ.get("FHIR_POSTGRES_DB", "fhir")
FHIR_POSTGRES_USER = os.environ.get("FHIR_POSTGRES_USER", "postgres")
FHIR_POSTGRES_PASSWORD = os.environ.get("FHIR_POSTGRES_PASSWORD", "postgres")
LOG_DIR = os.environ.get("LOG_DIR", "/etc/refInt/logging")
LOG_LEVEL = os.environ.get("LOG_LEVEL", 20)

HAPIFHIR_URL = os.environ.get("HAPIFHIR_URL", "http://ketos.ai:8080/gtfhir/base/")
DEBUG = os.environ.get("DEBUG", True)
WSHOST = os.environ.get("WSHOST", "0.0.0.0")
WSPORT = int(os.environ.get("WSPORT", 5000))
GTFHIR = bool(os.environ.get("GTFHIR", False))
