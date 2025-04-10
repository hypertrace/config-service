package ai.traceable.anomaly.config.service.trainer.trainingaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.trainer.PauseEntityLearnAction;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction.ActionCase;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UserRoleAction;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TrainingActionManagerTest {

  private static final String TRAINING_ACTION_DIRECTORY = "trainer/";
  private static final String SCOPED_TRAINING_ACTIONS_INPUT_FILE_PATH =
      TRAINING_ACTION_DIRECTORY + "scoped-training-actions-input.conf";
  // input TrainingAction for each scope that will be upserted
  private static final Config scopedTrainingActionsInput =
      ConfigFactory.parseResources(SCOPED_TRAINING_ACTIONS_INPUT_FILE_PATH);
  private static final String SCOPED_TRAINING_ACTIONS_OUTPUT_FILE_PATH =
      TRAINING_ACTION_DIRECTORY + "scoped-training-actions-output.conf";
  // output of upsertTrainingAction for each scope
  private static final Config scopedTrainingActionsOutput =
      ConfigFactory.parseResources(SCOPED_TRAINING_ACTIONS_OUTPUT_FILE_PATH);
  private static final String RESOLVED_TRAINING_ACTIONS_CONFIG_FILE_PATH =
      TRAINING_ACTION_DIRECTORY + "resolved-training-actions-config.conf";
  // output resolved ScopedTrainingActionConfig for each scope that are expected to be returned
  // as part of getAll method response
  private static final Config resolvedScopedTrainingActionsConfig =
      ConfigFactory.parseResources(RESOLVED_TRAINING_ACTIONS_CONFIG_FILE_PATH);
  // various config key names inside Config instance
  private static final String CUSTOMER_SCOPE_CONFIG = "customerScopeConfig";
  private static final String SERVICE_SCOPE_CONFIG = "serviceScopeConfig";
  private static final String ENVIRONMENT_SCOPE_CONFIG = "environmentScopeConfig";
  private static final String API_SCOPE_CONFIG = "apiScopeConfig";
  private static final String CONFIG_SCOPE_CONFIG = "configScope";
  private static final String TRAINING_ACTION_CONFIG = "trainingAction";
  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyEnvironmentScope environmentScope =
      AnomalyEnvironmentScope.newBuilder().setEnvironmentId("environment").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();
  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
  private final AnomalyConfigScope environmentConfigScope =
      AnomalyConfigScope.newBuilder().setEnvironmentScope(environmentScope).build();
  private TrainingActionManager actionManager;

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    TrainingActionConverter actionConverter = new TrainingActionConverter(Clock.systemUTC());
    AnomalyConfigScopeUtils anomalyConfigScopeUtils = new AnomalyConfigScopeUtils();
    this.actionManager =
        spy(
            new TrainingActionManagerImpl(
                actionConverter,
                configServiceBlockingStub,
                anomalyConfigScopeUtils,
                mock(ConfigChangeEventGenerator.class)));
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetTrainingActionForNonExistentScope() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              ScopedTrainingActionConfig result =
                  actionManager.getTrainingAction(requestContext, customerConfigScope);
              assertEquals(
                  ScopedTrainingActionConfig.newBuilder()
                      .setConfigScope(
                          AnomalyConfigScope.newBuilder()
                              .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                              .build())
                      .build(),
                  result,
                  "Should return customer scope with empty TrainingActionConfig");
            });
  }

  @Test
  void testGetTrainingActionForDifferentScopes() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    TrainingAction customerTrainingAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(true).build()))
            .build();
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () ->
                actionManager.upsertTrainingAction(
                    requestContext, customerConfigScope, customerTrainingAction));

    TrainingAction serviceTrainingAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(false).build()))
            .build();
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () ->
                actionManager.upsertTrainingAction(
                    requestContext, serviceConfigScope, serviceTrainingAction));

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              ScopedTrainingActionConfig customerResult =
                  actionManager.getTrainingAction(requestContext, customerConfigScope);
              assertTrue(
                  customerResult
                      .getTrainingActionConfigList()
                      .get(0)
                      .getTrainingAction()
                      .getUserRoleAction()
                      .getPauseEntityLearnAction()
                      .getDisabledAll());

              ScopedTrainingActionConfig serviceResult =
                  actionManager.getTrainingAction(requestContext, serviceConfigScope);
              assertFalse(
                  serviceResult
                      .getTrainingActionConfigList()
                      .get(0)
                      .getTrainingAction()
                      .getUserRoleAction()
                      .getPauseEntityLearnAction()
                      .getDisabledAll());
            });
  }

  @Test
  void testGetTrainingActionAfterMultipleUpsert() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    TrainingAction firstTrainingAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(true).build()))
            .build();
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () ->
                actionManager.upsertTrainingAction(
                    requestContext, customerConfigScope, firstTrainingAction));

    TrainingAction secondTrainingAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(false).build()))
            .build();
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () ->
                actionManager.upsertTrainingAction(
                    requestContext, customerConfigScope, secondTrainingAction));

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              ScopedTrainingActionConfig result =
                  actionManager.getTrainingAction(requestContext, customerConfigScope);
              assertFalse(
                  result
                      .getTrainingActionConfigList()
                      .get(0)
                      .getTrainingAction()
                      .getUserRoleAction()
                      .getPauseEntityLearnAction()
                      .getDisabledAll(),
                  "Latest upsert should override previous");
            });
  }

  @Test
  void testGetResolvedTrainingActionAcrossMultipleScopes() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    TrainingAction environmentAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(false).build()))
            .build();

    TrainingAction environmentAction2 =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(true).build()))
            .build();

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              actionManager.upsertTrainingAction(
                  requestContext, environmentConfigScope, environmentAction);
              actionManager.upsertTrainingAction(
                  requestContext, environmentConfigScope, environmentAction2);
            });

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              ScopedTrainingActionConfig resolvedConfig =
                  actionManager.getTrainingAction(requestContext, environmentConfigScope);

              assertTrue(
                  resolvedConfig
                      .getTrainingActionConfigList()
                      .get(0)
                      .getTrainingAction()
                      .getUserRoleAction()
                      .getPauseEntityLearnAction()
                      .getDisabledAll(),
                  "The environment scope should have the highest priority");
            });
  }

  @Test
  void testGetTrainingActionAfterPartialUpdate() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    TrainingAction originalAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(true).build()))
            .build();

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () ->
                actionManager.upsertTrainingAction(
                    requestContext, customerConfigScope, originalAction));

    TrainingAction updatedAction =
        TrainingAction.newBuilder()
            .setUserRoleAction(
                UserRoleAction.newBuilder()
                    .setPauseEntityLearnAction(
                        PauseEntityLearnAction.newBuilder().setDisabledAll(false).build()))
            .build();

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () ->
                actionManager.upsertTrainingAction(
                    requestContext, customerConfigScope, updatedAction));

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              ScopedTrainingActionConfig result =
                  actionManager.getTrainingAction(requestContext, customerConfigScope);
              assertFalse(
                  result
                      .getTrainingActionConfigList()
                      .get(0)
                      .getTrainingAction()
                      .getUserRoleAction()
                      .getPauseEntityLearnAction()
                      .getDisabledAll(),
                  "DisabledAll should be updated to false");
            });
  }

  @Test
  void testGetTrainingActionWithEmptyConfig() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .run(
            () -> {
              ScopedTrainingActionConfig result =
                  actionManager.getTrainingAction(requestContext, serviceConfigScope);
              assertEquals(
                  ScopedTrainingActionConfig.newBuilder()
                      .setConfigScope(
                          AnomalyConfigScope.newBuilder()
                              .setServiceScope(
                                  AnomalyServiceScope.newBuilder().setId("service").build())
                              .build())
                      .build(),
                  result,
                  "Should return service scope with empty TrainingActionConfig");
            });
  }

  @Test
  void testUpsertAndGetAllScopedTrainingAction() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    List<ScopedTrainingActionConfig> scopedTrainingActionConfigList =
        actionManager.getAllTrainingActions(requestContext);
    assertTrue(scopedTrainingActionConfigList.isEmpty());

    // upsert action at customer level and verify responses for upsert and getAll methods
    Config customerScopedInputConfig = scopedTrainingActionsInput.getConfig(CUSTOMER_SCOPE_CONFIG);
    Config customerScopedOutputConfig =
        scopedTrainingActionsOutput.getConfig(CUSTOMER_SCOPE_CONFIG);
    Config resolvedCustomerScopedConfig =
        resolvedScopedTrainingActionsConfig.getConfig(CUSTOMER_SCOPE_CONFIG);
    upsertAndVerifyForScope(
        customerConfigScope,
        customerScopedInputConfig,
        customerScopedOutputConfig,
        resolvedCustomerScopedConfig,
        requestContext);

    // upsert action at environment level and verify responses for upsert and getAll methods
    Config environmentScopedInputConfig =
        scopedTrainingActionsInput.getConfig(ENVIRONMENT_SCOPE_CONFIG);
    Config environmentScopedOutputConfig =
        scopedTrainingActionsOutput.getConfig(ENVIRONMENT_SCOPE_CONFIG);
    Config resolvedEnvironmentScopedConfig =
        resolvedScopedTrainingActionsConfig.getConfig(ENVIRONMENT_SCOPE_CONFIG);
    upsertAndVerifyForScope(
        environmentConfigScope,
        environmentScopedInputConfig,
        environmentScopedOutputConfig,
        resolvedEnvironmentScopedConfig,
        requestContext);

    // upsert action at service level and verify responses for upsert and getAll methods
    Config serviceScopedInputConfig = scopedTrainingActionsInput.getConfig(SERVICE_SCOPE_CONFIG);
    Config serviceScopedOutputConfig = scopedTrainingActionsOutput.getConfig(SERVICE_SCOPE_CONFIG);
    Config resolvedServiceScopedConfig =
        resolvedScopedTrainingActionsConfig.getConfig(SERVICE_SCOPE_CONFIG);
    upsertAndVerifyForScope(
        serviceConfigScope,
        serviceScopedInputConfig,
        serviceScopedOutputConfig,
        resolvedServiceScopedConfig,
        requestContext);

    // upsert action at api level and verify responses for upsert and getAll methods
    Config apiScopedInputConfig = scopedTrainingActionsInput.getConfig(API_SCOPE_CONFIG);
    Config apiScopedOutputConfig = scopedTrainingActionsOutput.getConfig(API_SCOPE_CONFIG);
    Config resolvedApiScopedConfig =
        resolvedScopedTrainingActionsConfig.getConfig(API_SCOPE_CONFIG);
    upsertAndVerifyForScope(
        apiConfigScope,
        apiScopedInputConfig,
        apiScopedOutputConfig,
        resolvedApiScopedConfig,
        requestContext);
  }

  private void upsertAndVerifyForScope(
      AnomalyConfigScope configScope,
      Config scopedInputConfig,
      Config scopedOutputConfig,
      Config scopedResolvedConfig,
      RequestContext requestContext) {
    Context.current().withValue(RequestContext.CURRENT, requestContext);
    requestContext.run(
        () -> {
          try {
            AnomalyConfigScope anomalyConfigScope =
                getAnomalyConfigScope(scopedInputConfig.getConfig(CONFIG_SCOPE_CONFIG));
            TrainingAction inputTrainingAction =
                getTrainingAction(scopedInputConfig.getConfig(TRAINING_ACTION_CONFIG));
            // make upsertTrainingAction call and validate the response
            ScopedTrainingActionConfig fetchedUpsertActionConfig =
                actionManager.upsertTrainingAction(
                    requestContext, anomalyConfigScope, inputTrainingAction);
            ScopedTrainingActionConfig expectedUpsertActionConfig =
                getScopedTrainingActionConfig(scopedOutputConfig);
            verifyScopedActionConfig(fetchedUpsertActionConfig, expectedUpsertActionConfig);

            // make getAllTrainingActions call and validate the response
            List<ScopedTrainingActionConfig> scopedTrainingActionConfigList =
                actionManager.getAllTrainingActions(requestContext);
            ScopedTrainingActionConfig scopedActionConfig =
                getScopedActionConfig(configScope, scopedTrainingActionConfigList);

            ScopedTrainingActionConfig scopeResolvedActionConfig =
                getScopedTrainingActionConfig(scopedResolvedConfig);
            verifyScopedActionConfig(scopedActionConfig, scopeResolvedActionConfig);
          } catch (InvalidProtocolBufferException ex) {
            fail();
          }
        });
  }

  private AnomalyConfigScope getAnomalyConfigScope(Config config)
      throws InvalidProtocolBufferException {
    AnomalyConfigScope.Builder builder = AnomalyConfigScope.newBuilder();
    JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(config.root().render(ConfigRenderOptions.concise()), builder);
    return builder.build();
  }

  private TrainingAction getTrainingAction(Config config) throws InvalidProtocolBufferException {
    TrainingAction.Builder builder = TrainingAction.newBuilder();
    JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(config.root().render(ConfigRenderOptions.concise()), builder);
    return builder.build();
  }

  private ScopedTrainingActionConfig getScopedTrainingActionConfig(Config resolvedScopedConfig)
      throws InvalidProtocolBufferException {
    ScopedTrainingActionConfig.Builder builder = ScopedTrainingActionConfig.newBuilder();
    JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(resolvedScopedConfig.root().render(ConfigRenderOptions.concise()), builder);
    return builder.build();
  }

  private ScopedTrainingActionConfig getScopedActionConfig(
      AnomalyConfigScope configScope, List<ScopedTrainingActionConfig> scopedActionConfigs) {

    for (ScopedTrainingActionConfig scopedActionConfig : scopedActionConfigs) {
      if (scopedActionConfig.getConfigScope().equals(configScope)) {
        return scopedActionConfig;
      }
    }
    return null;
  }

  private void verifyScopedActionConfig(
      ScopedTrainingActionConfig fetchedScopedActionConfig,
      ScopedTrainingActionConfig resolvedScopedActionConfig) {
    // verify that anomalyConfigScope is same
    assertEquals(
        fetchedScopedActionConfig.getConfigScope(), resolvedScopedActionConfig.getConfigScope());
    Map<ActionCase, TrainingActionConfig> fetchedActionConfigMap =
        getActionConfigMap(fetchedScopedActionConfig);
    Map<ActionCase, TrainingActionConfig> resolvedActionConfigMap =
        getActionConfigMap(resolvedScopedActionConfig);
    assertEquals(fetchedActionConfigMap.size(), resolvedActionConfigMap.size());
    // verify each TrainingAction within TrainingActionConfig is the same. Note that we cannot
    // verify the timestamp within fetchedScopedActionConfig since that is populated at runtime
    // within upsert implementation.
    fetchedActionConfigMap.forEach(
        (actionCase, fetchedActionConfig) -> {
          TrainingActionConfig resolvedActionConfig = resolvedActionConfigMap.get(actionCase);
          assertEquals(
              fetchedActionConfig.getTrainingAction(), resolvedActionConfig.getTrainingAction());
        });
  }

  private Map<ActionCase, TrainingActionConfig> getActionConfigMap(
      ScopedTrainingActionConfig scopedActionConfig) {
    return scopedActionConfig.getTrainingActionConfigList().stream()
        .collect(
            Collectors.toMap(
                trainingActionConfig -> trainingActionConfig.getTrainingAction().getActionCase(),
                Function.identity()));
  }
}
