package ai.traceable.config.service.splunk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.splunk.integration.config.service.api.v1.*;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class SplunkIntegrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static SplunkIntegrationConfigServiceGrpc.SplunkIntegrationConfigServiceBlockingStub
      splunkIntegrationConfigServiceBlockingStub;
  private RequestContext requestContext;

  @BeforeAll
  static void init() {
    splunkIntegrationConfigServiceBlockingStub =
        SplunkIntegrationConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testGetAllIntegrationsWhenEmpty() {
    requestContext = RequestContext.forTenantId("testSensitiveDataConfigService-tenant");
    int actual =
        requestContext
            .call(
                () ->
                    splunkIntegrationConfigServiceBlockingStub.getSplunkIntegrations(
                        GetSplunkIntegrationsRequest.newBuilder().build()))
            .getIntegrationsCount();
    assertEquals(0, actual);
  }

  @Test
  public void testGetAllIntegrations() {
    requestContext = RequestContext.forTenantId("testSensitiveDataConfigService-tenant");
    CreateSplunkIntegrationRequest request1 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setApiToken(EncryptedText.newBuilder().setKeyId("keyid").setValue("encrypted").build())
            .setName("name1")
            .setDescription("desc")
            .setHttpEventCollectorUrl("https://testhost.com/hec.url")
            .build();
    CreateSplunkIntegrationRequest request2 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setApiToken(EncryptedText.newBuilder().setKeyId("keyid").setValue("encrypted").build())
            .setName("name2")
            .setDescription("desc")
            .setHttpEventCollectorUrl("https://testhost.com/hec.url")
            .build();
    SplunkIntegration integration1 = createIntegration(requestContext, request1);
    SplunkIntegration integration2 = createIntegration(requestContext, request2);
    int actual =
        requestContext
            .call(
                () ->
                    splunkIntegrationConfigServiceBlockingStub.getSplunkIntegrations(
                        GetSplunkIntegrationsRequest.newBuilder().build()))
            .getIntegrationsCount();
    assertEquals(2, actual);
  }

  @Test
  public void testUpdateIntegration() {
    requestContext = RequestContext.forTenantId("testSensitiveDataConfigService-tenant");
    CreateSplunkIntegrationRequest request1 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setApiToken(EncryptedText.newBuilder().setKeyId("keyid").setValue("encrypted").build())
            .setName("name1")
            .setDescription("desc")
            .setHttpEventCollectorUrl("https://testhost.com/hec.url")
            .build();

    SplunkIntegration integration1 = createIntegration(requestContext, request1);
    UpdateSplunkIntegrationRequest updateSplunkIntegrationRequest =
        UpdateSplunkIntegrationRequest.newBuilder()
            .setId(integration1.getId())
            .setName("name2")
            .setDescription("desc")
            .setApiToken(EncryptedText.newBuilder().setKeyId("keyid").setValue("encrypted").build())
            .setHttpEventCollectorUrl("https://testhost.com/xyz")
            .build();
    SplunkIntegration updated =
        requestContext
            .call(
                () ->
                    splunkIntegrationConfigServiceBlockingStub.updateSplunkIntegration(
                        updateSplunkIntegrationRequest))
            .getIntegration();
    assertEquals("name2", updated.getDetails().getName());
  }

  private SplunkIntegration createIntegration(
      RequestContext requestContext, CreateSplunkIntegrationRequest request) {
    return requestContext
        .call(() -> splunkIntegrationConfigServiceBlockingStub.createSplunkIntegration(request))
        .getIntegration();
  }
}
