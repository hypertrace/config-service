## Config Service
Service for storing and serving configurations across multiple services and contexts.

## Description

[config service](https://github.com/hypertrace/config-service) is a place to store configuration data such as user preferences, saved query filters, ingestion config, etc. Many of these use cases have not yet been built out. In general, this service is meant for user-managed configuration that needs to be persisted, and contains support for version history, auditing etc. In the past, we've addressed such things by spinning up individual services (such as attribute service). As new features get built out, we want to avoid that (and eventually to merge older services back into this).

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/arch/ht-arch.png) | 
|:--:| 
| *Hypertrace Architecture* |

Refer [config_service.proto](config-service-api/src/main/proto/org/hypertrace/config/service/v1/config_service.proto) for Config Service APIs.

## Building locally
The Config service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Config Service, run:

```
./gradlew dockerBuildImages
```

## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 

### Running integration tests

Run `./gradlew integrationTest` to execute integration tests. 


### Testing image

To test your image using the docker-compose setup follow the steps:

- Commit you changes to a branch say `config-service-test`.
- Go to [hypertrace-service](https://github.com/hypertrace/hypertrace-service) and checkout the above branch in the submodule.
```
cd config-service && git checkout config-service-test && cd ..
```
- Change tag for `hypertrace-service` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-service:
    image: hypertrace/hypertrace-service:test
    container_name: hypertrace-service
    ...
```
- and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Config service](https://hub.docker.com/r/hypertrace/config-service)
