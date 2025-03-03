package ai.traceable.bot.categorized.policy.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStore;
import ai.traceable.bot.categorized.policy.service.v1.store.CategorizedBotConfigPolicyStoreManager;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CategorizedBotConfigPolicyServiceTest {

  private CategorizedBotConfigPolicyServiceGrpc.CategorizedBotConfigPolicyServiceBlockingStub stub;
  private MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @BeforeEach
  void beforeEach() {
    final UuidGenerator uuidGenerator = new UuidGenerator();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    final CategorizedBotConfigPolicyStoreManager categorizedBotConfigPolicyStoreManager =
        new CategorizedBotConfigPolicyStoreManager(
            new CategorizedBotConfigPolicyStore(
                configServiceBlockingStub, mockConfigChangeEventGenerator),
            uuidGenerator);
    mockGenericConfigService
        .addService(new CategorizedBotConfigPolicyService(categorizedBotConfigPolicyStoreManager))
        .start();
    this.stub =
        CategorizedBotConfigPolicyServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testCrud() {
    final RequestContext requestContext = RequestContext.forTenantId("t1");

    // Test Create
    final CategorizedBotConfigPolicy createdPolicy =
        requestContext
            .call(
                () ->
                    stub.createCategorizedBotConfigPolicy(
                        CreateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyDetails(
                                CategorizedBotConfigPolicyDetails.newBuilder()
                                    .setName("Test")
                                    .setDescription("Test")
                                    .setEnabled(false)
                                    .setCategorizedBotPolicyScope(
                                        CategorizedBotPolicyScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("Test")
                                                    .build())
                                            .build())
                                    .addCategorizedBotActionConfigs(
                                        CategorizedBotActionConfig.newBuilder()
                                            .setBotId("Test")
                                            .setBotAction(
                                                CategorizedBotAction.CATEGORIZED_BOT_ACTION_BLOCK)
                                            .setDisabled(false)
                                            .build())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();
    assertFalse(createdPolicy.getCategorizedBotPolicyDetails().getEnabled());

    // Test Get policy by id
    final List<CategorizedBotConfigPolicy> expectedPolicies =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder()
                                    .addBotConfigPolicyIds(createdPolicy.getId())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(1, expectedPolicies.size());
    assertEquals(createdPolicy, expectedPolicies.get(0));

    // Test Get policy all policies with empty filter
    final List<CategorizedBotConfigPolicy> totalPoliciesEmptyFilter =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder().build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(1, totalPoliciesEmptyFilter.size());

    // Test Get policy all policies with no filter
    final List<CategorizedBotConfigPolicy> totalPoliciesNoFilter =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder().build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(1, totalPoliciesNoFilter.size());

    // Test update policy
    final CategorizedBotConfigPolicyDetails updatedPolicyDetails =
        createdPolicy.getCategorizedBotPolicyDetails().newBuilderForType().setEnabled(true).build();
    final CategorizedBotConfigPolicy updatedPolicy =
        requestContext
            .call(
                () ->
                    stub.updateCategorizedBotConfigPolicy(
                        UpdateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicy(
                                CategorizedBotConfigPolicy.newBuilder()
                                    .setCategorizedBotPolicyDetails(updatedPolicyDetails)
                                    .setId(createdPolicy.getId())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();
    assertEquals(updatedPolicyDetails, updatedPolicy.getCategorizedBotPolicyDetails());
    assertTrue(updatedPolicy.getCategorizedBotPolicyDetails().getEnabled());

    // Test update invalid policy
    assertThrows(
        StatusRuntimeException.class,
        () ->
            requestContext.call(
                () ->
                    stub.updateCategorizedBotConfigPolicy(
                        UpdateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicy(
                                CategorizedBotConfigPolicy.newBuilder()
                                    .setCategorizedBotPolicyDetails(updatedPolicyDetails)
                                    .setId("dummy")
                                    .build())
                            .build())));

    // Test delete policy
    final CategorizedBotConfigPolicy deletedPolicy =
        requestContext
            .call(
                () ->
                    stub.deleteCategorizedBotConfigPolicy(
                        DeleteCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyId(updatedPolicy.getId())
                            .build()))
            .getCategorizedBotConfigPolicy();
    assertEquals(updatedPolicy.getId(), deletedPolicy.getId());

    final List<CategorizedBotConfigPolicy> allPolicies =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder().build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(0, allPolicies.size());

    // Test delete policy no policy id
    final CategorizedBotConfigPolicy secondCreatedPolicy =
        requestContext
            .call(
                () ->
                    stub.createCategorizedBotConfigPolicy(
                        CreateCategorizedBotConfigPolicyRequest.newBuilder()
                            .setCategorizedBotConfigPolicyDetails(
                                CategorizedBotConfigPolicyDetails.newBuilder()
                                    .setName("Test")
                                    .setDescription("Test")
                                    .setEnabled(false)
                                    .setCategorizedBotPolicyScope(
                                        CategorizedBotPolicyScope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironmentIds("Test")
                                                    .build())
                                            .build())
                                    .addCategorizedBotActionConfigs(
                                        CategorizedBotActionConfig.newBuilder()
                                            .setBotId("Test")
                                            .setBotAction(
                                                CategorizedBotAction.CATEGORIZED_BOT_ACTION_BLOCK)
                                            .setDisabled(false)
                                            .build())
                                    .build())
                            .build()))
            .getCategorizedBotConfigPolicy();

    final CategorizedBotConfigPolicy deletedSecondPolicy =
        requestContext
            .call(
                () ->
                    stub.deleteCategorizedBotConfigPolicy(
                        DeleteCategorizedBotConfigPolicyRequest.newBuilder().build()))
            .getCategorizedBotConfigPolicy();
    assertEquals(updatedPolicy.getId(), deletedPolicy.getId());

    final List<CategorizedBotConfigPolicy> allCurrentPolicies =
        requestContext
            .call(
                () ->
                    stub.getCategorizedBotConfigPolicies(
                        GetCategorizedBotConfigPoliciesRequest.newBuilder()
                            .setCategorizedBotConfigPolicyFilter(
                                CategorizedBotConfigPolicyFilter.newBuilder().build())
                            .build()))
            .getCategorizedBotConfigPoliciesList();
    assertEquals(1, allCurrentPolicies.size());
  }
}
