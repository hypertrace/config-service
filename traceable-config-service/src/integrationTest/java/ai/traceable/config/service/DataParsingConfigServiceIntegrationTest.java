package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.data.parsing.config.service.v1.AttributeFilter;
import ai.traceable.data.parsing.config.service.v1.CreateDataParsingRuleRequest;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfigServiceGrpc;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfigServiceGrpc.DataParsingConfigServiceBlockingStub;
import ai.traceable.data.parsing.config.service.v1.DataParsingRule;
import ai.traceable.data.parsing.config.service.v1.DeleteDataParsingRuleRequest;
import ai.traceable.data.parsing.config.service.v1.GetDataParsingRulesRequest;
import ai.traceable.data.parsing.config.service.v1.UpdateDataParsingRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DataParsingConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenantId");

  private static DataParsingConfigServiceBlockingStub stub;

  private static final DataParsingRule RULE =
      DataParsingRule.newBuilder()
          .setEnabled(true)
          .setMode(DataParsingRule.DataParsingMode.DATA_PARSING_MODE_URL_ENCODED)
          .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes("http.").build())
          .build();

  @BeforeAll
  static void init() {
    stub =
        DataParsingConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testCRUDDataParsingRule() {
    // Initially empty
    List<DataParsingConfig> configs = getConfigs();
    assertTrue(configs.isEmpty());

    // Create
    DataParsingConfig created = createConfig();
    assertNotNull(created.getId());
    assertEquals(RULE, created.getDataParsingRule());

    // Read
    configs = getConfigs();
    assertEquals(1, configs.size());
    assertEquals(created.toBuilder().setRank(1).build(), configs.get(0));

    // Update
    DataParsingRule updatedRule = RULE.toBuilder().setEnabled(false).build();
    DataParsingConfig updated = updateConfig(created.getId(), updatedRule);
    assertEquals(created.getId(), updated.getId());
    assertFalse(updated.getDataParsingRule().getEnabled());

    // Delete
    deleteConfig(created.getId());
    assertTrue(getConfigs().isEmpty());
  }

  private List<DataParsingConfig> getConfigs() {
    return REQUEST_CONTEXT.call(
        () ->
            stub.getDataParsingRules(GetDataParsingRulesRequest.getDefaultInstance())
                .getDataParsingConfigsList());
  }

  private DataParsingConfig createConfig() {
    CreateDataParsingRuleRequest request =
        CreateDataParsingRuleRequest.newBuilder()
            .setDataParsingRule(DataParsingConfigServiceIntegrationTest.RULE)
            .build();
    return REQUEST_CONTEXT.call(() -> stub.createDataParsingRule(request).getDataParsingConfig());
  }

  private DataParsingConfig updateConfig(String id, DataParsingRule rule) {
    UpdateDataParsingRuleRequest request =
        UpdateDataParsingRuleRequest.newBuilder().setId(id).setDataParsingRule(rule).build();
    return REQUEST_CONTEXT.call(() -> stub.updateDataParsingRule(request).getDataParsingConfig());
  }

  private void deleteConfig(String id) {
    DeleteDataParsingRuleRequest request =
        DeleteDataParsingRuleRequest.newBuilder().setId(id).build();
    REQUEST_CONTEXT.call(() -> stub.deleteDataParsingRule(request));
  }
}
