REGISTRY_PREFIX=docker.miracum.org/ketos
VERSION_TAG=:v0.3.0

printf "pulling image: $REGISTRY_PREFIX/ketos_env_ds:$VERSION_TAG \n"
docker pull $REGISTRY_PREFIX/ketos_env_ds$VERSION_TAG
docker tag $REGISTRY_PREFIX/ketos_env_ds$VERSION_TAG $REGISTRY_PREFIX/ketos_env_ds:latest
docker tag $REGISTRY_PREFIX/ketos_env_ds$VERSION_TAG ketos_env_ds:latest

printf "pulling image: $REGISTRY_PREFIX/ketos_env_r$VERSION_TAG \n"
docker pull $REGISTRY_PREFIX/ketos_env_r$VERSION_TAG
docker tag $REGISTRY_PREFIX/ketos_env_r$VERSION_TAG $REGISTRY_PREFIX/ketos_env_r:latest
docker tag $REGISTRY_PREFIX/ketos_env_r$VERSION_TAG ketos_env_r:latest

printf "finished pulling all images for DS-QP ....\n"
