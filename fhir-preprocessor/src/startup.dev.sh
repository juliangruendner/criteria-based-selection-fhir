#!/bin/bash
chmod +x wait-for-it.sh
./wait-for-it.sh "$POSTGRES_HOST:5432" --timeout=0
#pip install --upgrade pip
#pip3 install -r requirements.txt
#pip3 install fhirclient==$FHIR__CLIENT_VERSION
#pip3 install git+https://github.com/smart-on-fhir/client-py.git
python3 TaskWorkerRun.py &
python3 api.py