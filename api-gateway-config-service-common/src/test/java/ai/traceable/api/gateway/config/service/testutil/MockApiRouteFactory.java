package ai.traceable.api.gateway.config.service.testutil;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import java.util.UUID;

public interface MockApiRouteFactory {
  String API_PATH1 = "/planets/Mars";
  String API_PATH2 = "/planets/The_Red_Planet";

  String ORG_ID1 = "org1-id";
  String ORG_ID2 = "org2-id";

  String SERVICE1 = "Service 1";
  String SERVICE2 = "Service 2";

  String METHOD1 = "GET";
  String METHOD2 = "POST";

  ApiRoute ROUTE_WITH_ALL_FIELDS =
      ApiRoute.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setMetadata(Metadata.newBuilder().setOrgId(ORG_ID1))
          .setInfo(
              RouteInfo.newBuilder()
                  .setPath(API_PATH1)
                  .setHttpMethod(METHOD1)
                  .setServiceName(SERVICE1)
                  .setIsDeprecated(true))
          .build();

  ApiRoute ROUTE_WITHOUT_METHOD =
      ApiRoute.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setMetadata(Metadata.newBuilder().setOrgId(ORG_ID1))
          .setInfo(
              RouteInfo.newBuilder()
                  .setPath(API_PATH2)
                  .setServiceName(SERVICE2)
                  .setIsDeprecated(true))
          .build();

  ApiRoute ROUTE_WITHOUT_SERVICE =
      ApiRoute.newBuilder()
          .setId(UUID.randomUUID().toString())
          .setMetadata(Metadata.newBuilder().setOrgId(ORG_ID2))
          .setInfo(
              RouteInfo.newBuilder()
                  .setPath(API_PATH1)
                  .setHttpMethod(METHOD2)
                  .setIsDeprecated(false))
          .build();
}
