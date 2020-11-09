source ./ketos.config

if [ ! "$(docker network ls | grep hbBackend)" ]; then
docker network create hbBackend
fi

printf "######################\nInitialising Ketos ...\n######################\n\n"

docker-compose up -d
