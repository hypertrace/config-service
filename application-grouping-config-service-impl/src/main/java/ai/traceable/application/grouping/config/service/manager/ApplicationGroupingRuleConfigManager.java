package ai.traceable.application.grouping.config.service.manager;

import ai.traceable.application.grouping.config.service.store.ApplicationGroupingRuleConfigStore;
import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfig;
import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfigInfo;
import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigRequest;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Status;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApplicationGroupingRuleConfigManager implements ApplicationGroupingRuleConfigService {
  private static final String APPLICATION_GROUPING_CONFIG_SERVICE_PATH =
      "application.grouping.config.service";
  private static final String MAX_APPLICATION_GROUPING_RULES_PER_TENANT_PATH =
      "maxApplicationGroupingRulesPerTenant";
  private static final int DEFAULT_MAX_APPLICATION_GROUPING_RULES_PER_TENANT = 100;

  private final ApplicationGroupingRuleConfigStore applicationGroupingRuleConfigStore;
  private final UuidGenerator uuidGenerator;
  private final Config config;

  @Inject
  public ApplicationGroupingRuleConfigManager(
      final ApplicationGroupingRuleConfigStore applicationGroupingRuleConfigStore,
      final UuidGenerator uuidGenerator,
      final Config config) {
    this.applicationGroupingRuleConfigStore = applicationGroupingRuleConfigStore;
    this.uuidGenerator = uuidGenerator;
    this.config =
        config.hasPath(APPLICATION_GROUPING_CONFIG_SERVICE_PATH)
            ? config.getConfig(APPLICATION_GROUPING_CONFIG_SERVICE_PATH)
            : ConfigFactory.empty();
  }

  @Override
  public List<ApplicationGroupingRuleConfig> getAllApplicationGroupingRuleConfigs(
      final RequestContext requestContext,
      final GetAllApplicationGroupingRuleConfigsRequest request) {
    return applicationGroupingRuleConfigStore.getMatchingApplicationGroupingRuleConfigs(
        requestContext, request.getFilter());
  }

  @Override
  public ApplicationGroupingRuleConfig createApplicationGroupingRuleConfig(
      final RequestContext requestContext,
      final CreateApplicationGroupingRuleConfigRequest request) {
    validateApplicationGroupingRuleLimit(requestContext);
    final String id = uuidGenerator.generateRandomId();
    final ApplicationGroupingRuleConfig applicationGroupingRuleConfig =
        buildApplicationGroupingRuleConfig(id, request.getApplicationGroupingRuleConfigInfo());
    return applicationGroupingRuleConfigStore
        .upsertObject(requestContext, applicationGroupingRuleConfig)
        .getData();
  }

  @Override
  public ApplicationGroupingRuleConfig updateApplicationGroupingRuleConfig(
      final RequestContext requestContext,
      final UpdateApplicationGroupingRuleConfigRequest request) {
    final ApplicationGroupingRuleConfig applicationGroupingRuleConfig =
        buildApplicationGroupingRuleConfig(
            request.getId(), request.getApplicationGroupingRuleConfigInfo());
    return applicationGroupingRuleConfigStore
        .upsertObject(requestContext, applicationGroupingRuleConfig)
        .getData();
  }

  @Override
  public void deleteApplicationGroupingRuleConfigs(
      final RequestContext requestContext,
      final DeleteApplicationGroupingRuleConfigsRequest request) {
    applicationGroupingRuleConfigStore.deleteObjects(requestContext, request.getIdsList());
  }

  @Override
  public boolean doesApplicationGroupingRuleConfigExist(
      final RequestContext requestContext, final String id) {
    return applicationGroupingRuleConfigStore.getData(requestContext, id).isPresent();
  }

  private ApplicationGroupingRuleConfig buildApplicationGroupingRuleConfig(
      final String id, final ApplicationGroupingRuleConfigInfo configInfo) {
    return ApplicationGroupingRuleConfig.newBuilder()
        .setId(id)
        .setApplicationGroupingRuleConfigInfo(configInfo)
        .build();
  }

  private void validateApplicationGroupingRuleLimit(RequestContext requestContext) {
    int existingRulesCount =
        applicationGroupingRuleConfigStore.getAllConfigData(requestContext).size();
    int maxRulesPerTenant =
        config.hasPath(MAX_APPLICATION_GROUPING_RULES_PER_TENANT_PATH)
            ? config.getInt(MAX_APPLICATION_GROUPING_RULES_PER_TENANT_PATH)
            : DEFAULT_MAX_APPLICATION_GROUPING_RULES_PER_TENANT;
    if (existingRulesCount >= maxRulesPerTenant) {
      log.warn(
          "Tenant {} has reached the maximum limit of {} application grouping rules",
          requestContext.getTenantId().orElseThrow(),
          maxRulesPerTenant);
      throw ContextualStatusExceptionBuilder.from(
              Status.RESOURCE_EXHAUSTED.withDescription(
                  String.format(
                      "You've reached the maximum limit of %d application rules. Delete an existing rule to create a new one.",
                      maxRulesPerTenant)))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }
}
