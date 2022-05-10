## Mock config service
This service is a part of Mock Platform required by the quality team

It serves the configs which are required by the TPA, the configs are test dependent and passed as runtime
following are the configs:
1. blocking-config-rule.json
2. external-user-attribution-config-rule.json
3. get-api-naming-rule.json
4. get-local-processing-config-rule.json
5. get-span-processing-rule.json
6. pii-filter-config-rule.json

## setting up service
1. Pull docker image from jfrog, the image is available in docker-quality registry on jfrog.\
   PATH: mock-platform/mock-config-service
2. The configs are different for different tests so it is passed to container during runtime.
    1. create a directory config-svc-data containing your configs\
    2. we need to mount the directory during docker run command as \
       ```docker run -p <p1>:<p2> -v <absolute-dir-path>/config-svc-data:/app/config-svc-data <image> ```
