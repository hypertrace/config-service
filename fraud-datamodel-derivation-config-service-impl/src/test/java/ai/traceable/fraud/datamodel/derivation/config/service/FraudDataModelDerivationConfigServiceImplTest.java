package ai.traceable.fraud.datamodel.derivation.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStore;
import ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStoreManager;
import ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationTransformConfigStore;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateUserAgentMergeMappingConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DeleteUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DeleteUserAgentMergeMappingConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfigType;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.FraudDataModelDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetUserAgentMergeMappingConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetUserAgentMergeMappingConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetUserAgentMergeMappingConfigsResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateUserAgentMergeMappingConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateUserAgentMergeMappingConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UserAgentMergeMappingConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.validation.FraudDataModelDerivationConfigRequestValidator;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FraudDataModelDerivationConfigServiceImplTest {
  private FraudDataModelDerivationConfigServiceGrpc
          .FraudDataModelDerivationConfigServiceBlockingStub
      fraudDataModelDerivationConfigServiceBlockingStub;
  private FraudDataModelDerivationConfigStoreManager storeManager;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;

  @BeforeEach
  void beforeEach() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.storeManager =
        new FraudDataModelDerivationConfigStoreManager(
            new FraudDataModelDerivationConfigStore(genericStub, eventGenerator),
            new FraudDataModelDerivationTransformConfigStore(genericStub, eventGenerator),
            uuidGenerator);
    this.mockGenericConfigService
        .addService(
            new FraudDataModelDerivationConfigServiceImpl(
                storeManager, new FraudDataModelDerivationConfigRequestValidator()))
        .start();

    this.fraudDataModelDerivationConfigServiceBlockingStub =
        FraudDataModelDerivationConfigServiceGrpc.newBlockingStub(
                this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testDerivationConfigCRUD() {
    RequestContext requestContext = buildRequestContext();
    DerivationConfig createdDerivationConfig =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .createDerivationConfig(
                        CreateDerivationConfigRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                            .setName("derivation_config")
                            .setDerivationConfig("yaml_config")
                            .build())
                    .getDerivationConfig());

    Assertions.assertNotNull(createdDerivationConfig.getId());
    String uuid = createdDerivationConfig.getId();

    DerivationConfig updatedDerivationConfig =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .updateDerivationConfig(
                        UpdateDerivationConfigRequest.newBuilder()
                            .setDerivationConfig(
                                DerivationConfig.newBuilder()
                                    .setId(uuid)
                                    .setDerivationConfigType(
                                        DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                                    .setName("derivation_config_updated")
                                    .setDerivationConfig("yaml_config_updated"))
                            .build())
                    .getDerivationConfig());

    assertEquals("derivation_config_updated", updatedDerivationConfig.getName());
    assertEquals("yaml_config_updated", updatedDerivationConfig.getDerivationConfig());

    GetDerivationConfigResponse getDerivationConfigResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfig(
                    GetDerivationConfigRequest.newBuilder()
                        .setDerivationConfigId(uuid)
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                        .build()));
    Assertions.assertNotNull(getDerivationConfigResponse.getDerivationConfig());
    Assertions.assertEquals(uuid, getDerivationConfigResponse.getDerivationConfig().getId());

    GetDerivationConfigsResponse getDerivationConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfigs(
                    GetDerivationConfigsRequest.newBuilder()
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                        .build()));
    assertEquals(1, getDerivationConfigsResponse.getDerivationConfigsCount());

    getDerivationConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfigs(
                    GetDerivationConfigsRequest.newBuilder()
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_ENTITY)
                        .build()));
    assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());

    storeManager.deleteDerivedConfig(requestContext, uuid);

    getDerivationConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfigs(
                    GetDerivationConfigsRequest.newBuilder()
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                        .build()));
    assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());
  }

  @Test
  public void testUserAgentMappingConfigCrud() {
    RequestContext requestContext = buildRequestContext();

    Map<String, List<String>> map = Map.of("a", List.of("b", "c"), "d", List.of("e", "f"));
    UserAgentMergeMappingConfig config = convertToUserAgentMergeMappingConfig(map, null);

    CreateUserAgentMergeMappingConfigResponse createUserAgentMergeMappingConfigResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .createUserAgentMergeMappingConfig(
                        CreateUserAgentMergeMappingConfigRequest.newBuilder()
                            .putAllPairs(config.getPairsMap())
                            .build()));
    assertNotNull(createUserAgentMergeMappingConfigResponse.getUserAgentMergeMappingConfig());
    String id = createUserAgentMergeMappingConfigResponse.getUserAgentMergeMappingConfig().getId();
    System.out.println("Created config with id=" + id);

    map = Map.of("a1", List.of("b1", "c1"), "d1", List.of("e1", "f1"));
    UserAgentMergeMappingConfig updateConfig = convertToUserAgentMergeMappingConfig(map, id);

    UpdateUserAgentMergeMappingConfigResponse updateUserAgentMergeMappingConfigResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .updateUserAgentMergeMappingConfig(
                        UpdateUserAgentMergeMappingConfigRequest.newBuilder()
                            .setUserAgentMergeMappingConfig(updateConfig)
                            .build()));

    assertEquals(
        id, updateUserAgentMergeMappingConfigResponse.getUserAgentMergeMappingConfig().getId());
    assertEquals(
        map,
        convertToMap(
            updateUserAgentMergeMappingConfigResponse
                .getUserAgentMergeMappingConfig()
                .getPairsMap()));

    GetUserAgentMergeMappingConfigResponse getUserAgentMergeMappingConfigResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .getUserAgentMergeMappingConfig(
                        GetUserAgentMergeMappingConfigRequest.newBuilder().setId(id).build()));

    assertEquals(
        id, getUserAgentMergeMappingConfigResponse.getUserAgentMergeMappingConfig().getId());

    DeleteUserAgentMergeMappingConfigResponse deleteUserAgentMergeMappingConfigResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .deleteUserAgentMergeMappingConfig(
                        DeleteUserAgentMergeMappingConfigRequest.newBuilder()
                            .setId(
                                createUserAgentMergeMappingConfigResponse
                                    .getUserAgentMergeMappingConfig()
                                    .getId())
                            .build()));

    GetUserAgentMergeMappingConfigsResponse getUserAgentMergeMappingConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .getUserAgentMergeMappingConfigs(
                        GetUserAgentMergeMappingConfigsRequest.newBuilder().build()));

    assertEquals(0, getUserAgentMergeMappingConfigsResponse.getUserAgentMergeMappingConfigCount());
  }

  private UserAgentMergeMappingConfig convertToUserAgentMergeMappingConfig(
      Map<String, List<String>> map, String id) {
    UserAgentMergeMappingConfig.Builder builder = UserAgentMergeMappingConfig.newBuilder();
    if (id != null) {
      builder.setId(id);
    }

    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      String key = entry.getKey();
      List<String> values = entry.getValue();

      ListValue.Builder listValueBuilder = ListValue.newBuilder();
      listValueBuilder.addAllValues(
          values.stream()
              .map(value -> Value.newBuilder().setStringValue(value).build())
              .collect(Collectors.toList()));

      builder.putPairs(key, listValueBuilder.build());
    }

    return builder.build();
  }

  private Map<String, List<String>> convertToMap(Map<String, ListValue> listValueMap) {
    Map<String, List<String>> stringListMap = new HashMap<>();

    for (Map.Entry<String, ListValue> entry : listValueMap.entrySet()) {
      String key = entry.getKey();
      ListValue listValue = entry.getValue();

      List<String> values =
          listValue.getValuesList().stream()
              .map(Value::getStringValue)
              .collect(Collectors.toList());

      stringListMap.put(key, values);
    }

    return stringListMap;
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1")
        .put(
            "authorization",
            "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJGYXZpYW4uUmV5bm9sZHNAZXhhbXBsZS5jb20iLCJyb2xlIjoidXNlciIsImlhdCI6MTY4NDM0NzcxOSwiZXhwIjoxNjg0OTUyNTE5fQ.cjyK-u3j7K5sHNSf7SG0oGe6xKCV0jOTm9kqN68JmFkdxUx5Dvd1WuF7Zg7uM-8sNBgxmtPg-bxJ2Ddnmb4Fd3IhSe1L-1dbZi3-dJldlK6i9m3O5pp3ivoQvobk1mfh2jCdbZ3tLcLF7t5OLDnqK_9COQ4plTMlmwX8Jr1L8C1kfzYHfHa91Rc9lDGjSJnxaAwfXAgqBhOSZCdX-EgYRyINnF6elLibLnL8J_PP50RLctNnZuEznImvPXQ6twl8A6JJmCkJYUp0HP9mdBJIz6RcuHYt0QU5avZprKQ6p_23WhuNzSvPRSjD0l9RGR8uQHU8LWVuu9i0L8Sfyr3TrA");
  }
}
