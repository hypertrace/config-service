package ai.traceable.localprocessing.config.service.apinaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.client.EntityDataServiceClient;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.ServiceResponse;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultApiNamingManagerTest {
  private ApiNamingManager apiNamingManager;
  private EntityDataServiceClient entityDataServiceClient;

  @BeforeEach
  void setup() {
    TrainerConfigServiceBlockingStub configServiceBlockingStub =
        mock(TrainerConfigServiceBlockingStub.class);
    entityDataServiceClient = mock(EntityDataServiceClient.class);
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingConfig apiNamingConfig = mock(ApiNamingConfig.class);
    apiNamingManager =
        new DefaultApiNamingManager(
            configServiceBlockingStub, entityDataServiceClient, apiNamingConfig, uuidGenerator);
    when(configServiceBlockingStub.getAllScopedTrainingConfigs(any()))
        .thenReturn(ApiNamingManagerTestUtils.buildGetAllScopedTrainingConfigsResponse());
    when(apiNamingConfig.getFallbackRegexes())
        .thenReturn(
            List.of(
                "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
                "\\d+"));
  }

  @Test
  void testGetServiceResponseList() {
    ai.traceable.localprocessing.config.service.v1.ApiNamingConfig apiNamingConfigServiceLevel =
        ApiNamingManagerTestUtils.buildApiNamingConfig();
    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder().setServiceName("serviceName1").setConfigHash("").build();
    ServiceRequest serviceRequest2 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName2")
            .setConfigHash(apiNamingConfigServiceLevel.getHash())
            .build();
    when(entityDataServiceClient.getByTypeAndIdentifyingProperties(
            any(),
            eq(
                ApiNamingManagerTestUtils.buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    "serviceName1"))))
        .thenReturn(Entity.newBuilder().setEntityId("serviceId1").build());
    when(entityDataServiceClient.getByTypeAndIdentifyingProperties(
            any(),
            eq(
                ApiNamingManagerTestUtils.buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    "serviceName2"))))
        .thenReturn(Entity.newBuilder().setEntityId("serviceId2").build());

    List<ServiceResponse> actualServiceResponseList =
        apiNamingManager.getServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1, serviceRequest2))
                .build());
    assertEquals(2, actualServiceResponseList.size());
    assertTrue(
        actualServiceResponseList.contains(
            ServiceResponse.newBuilder()
                .setServiceName("serviceName1")
                .setConfig(apiNamingConfigServiceLevel)
                .build()));
    assertTrue(
        actualServiceResponseList.contains(
            ServiceResponse.newBuilder()
                .setServiceName("serviceName2")
                .setConfig(
                    ai.traceable.localprocessing.config.service.v1.ApiNamingConfig.newBuilder()
                        .setHash(apiNamingConfigServiceLevel.getHash())
                        .build())
                .build()));
  }
}
