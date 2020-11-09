if [ ! "$(docker network ls | grep hbBackend)" ]; then
docker network create hbBackend
fi

if [ ! "$(docker network ls | grep ketos_environment)" ]; then
docker network create ketos_environment
fi

cd deploy/test-db
docker-compose up -d

cd ../../fhir-preprocessor
docker-compose up -d