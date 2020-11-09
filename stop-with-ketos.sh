if [ ! "$(docker network ls | grep hbBackend)" ]; then
docker network create hbBackend
fi

if [ ! "$(docker network ls | grep ketos_environment)" ]; then
docker network create ketos_environment
fi

cd deploy/test-db
docker-compose stop

cd ../../fhir-preprocessor
docker-compose stop

cd ../deploy/ketos
bash pullKetosNotebooks.sh
docker-compose stop