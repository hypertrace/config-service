package ai.traceable.config.service.fraud.datamodel.derivation;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfigType;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.FraudDataModelDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import com.google.common.io.Resources;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FraudDataModelDerivationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static FraudDataModelDerivationConfigServiceGrpc
          .FraudDataModelDerivationConfigServiceBlockingStub
      serviceStub;

  @BeforeAll
  static void init() {
    serviceStub =
        FraudDataModelDerivationConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testCRUD() throws IOException {
    URL resource = Resources.getResource("fraud/datamodel/derivation/event_derivation.yaml");
    String yamlConfig = Resources.toString(resource, Charset.defaultCharset());
    CreateDerivationConfigRequest createDerivationConfigRequest =
        CreateDerivationConfigRequest.newBuilder()
            .setName("event_derivation")
            .setDerivationConfig(yamlConfig)
            .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
            .build();
    var createDerivationConfigResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.createDerivationConfig(createDerivationConfigRequest));
    Assertions.assertTrue(createDerivationConfigResponse.hasDerivationConfig());
    DerivationConfig config = createDerivationConfigResponse.getDerivationConfig();
    Assertions.assertFalse(StringUtils.isBlank(config.getId()));
    Assertions.assertEquals(createDerivationConfigRequest.getName(), config.getName());
    Assertions.assertEquals(
        createDerivationConfigRequest.getDerivationConfigType(), config.getDerivationConfigType());
    Assertions.assertEquals(
        createDerivationConfigRequest.getDerivationConfig(), config.getDerivationConfig());

    DerivationConfig updatedConfig = config.toBuilder().setName("updated_config").build();
    UpdateDerivationConfigRequest updateDerivationConfigRequest =
        UpdateDerivationConfigRequest.newBuilder().setDerivationConfig(updatedConfig).build();
    var updateDerivationConfigResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.updateDerivationConfig(updateDerivationConfigRequest));
    Assertions.assertEquals(
        updatedConfig.getName(), updateDerivationConfigResponse.getDerivationConfig().getName());

    var getDerivationConfigsResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getDerivationConfigs(
                        GetDerivationConfigsRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                            .build()));
    Assertions.assertEquals(1, getDerivationConfigsResponse.getDerivationConfigsCount());
    Assertions.assertEquals(updatedConfig, getDerivationConfigsResponse.getDerivationConfigs(0));

    getDerivationConfigsResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getDerivationConfigs(
                        GetDerivationConfigsRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_ENTITY)
                            .build()));
    Assertions.assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());

    getDerivationConfigsResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getDerivationConfigs(
                        GetDerivationConfigsRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_RELATIONSHIP)
                            .build()));
    Assertions.assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());

    getDerivationConfigsResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getDerivationConfigs(
                        GetDerivationConfigsRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                            .addDerivationConfigIds(updatedConfig.getId())
                            .build()));
    Assertions.assertEquals(1, getDerivationConfigsResponse.getDerivationConfigsCount());
    Assertions.assertEquals(updatedConfig, getDerivationConfigsResponse.getDerivationConfigs(0));

    getDerivationConfigsResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getDerivationConfigs(
                        GetDerivationConfigsRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                            .addDerivationConfigIds("non_existent")
                            .build()));
    Assertions.assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());

    var derivationConfigResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getDerivationConfig(
                        GetDerivationConfigRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                            .setDerivationConfigId(updatedConfig.getId())
                            .build()));
    Assertions.assertEquals(updatedConfig, derivationConfigResponse.getDerivationConfig());
  }

  @Test
  public void testUpdateFailsOnNonExistentConfig() {
    UpdateDerivationConfigRequest updateDerivationConfigRequest =
        UpdateDerivationConfigRequest.newBuilder()
            .setDerivationConfig(DerivationConfig.getDefaultInstance())
            .build();
    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.updateDerivationConfig(updateDerivationConfigRequest));
      Assertions.fail("Expected update to fail");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(e.getStatus(), Status.NOT_FOUND);
    }
  }
}
