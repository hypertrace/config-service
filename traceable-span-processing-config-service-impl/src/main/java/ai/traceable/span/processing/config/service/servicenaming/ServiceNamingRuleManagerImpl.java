package ai.traceable.span.processing.config.service.servicenaming;

import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleFilter;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
class ServiceNamingRuleManagerImpl implements ServiceNamingRulesManager {
  private final ServiceNamingRuleStore ruleStore;
  private final ServiceNamingRequestValidator validator;
  private final ServiceNamingRuleBuilder ruleBuilder;

  @Override
  public List<ServiceNamingRule> getRules(
      RequestContext requestContext, ServiceNamingRuleFilter filter) {
    this.validator.validateOrThrow(requestContext);
    return this.ruleStore.getAllObjects(requestContext, filter).stream()
        .map(this.ruleBuilder::buildFromConfigObject)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteRule(RequestContext requestContext, String id) {
    this.validator.validateOrThrow(requestContext);
    this.ruleStore.getData(requestContext, id).orElseThrow(Status.NOT_FOUND::asRuntimeException);
    this.ruleStore.deleteObject(requestContext, id);
  }

  @Override
  public ServiceNamingRule updateRule(
      RequestContext requestContext, UpdateServiceNamingRuleRequest updateRequest) {
    this.validator.validateOrThrow(requestContext, updateRequest);
    ServiceNamingRule existingRule =
        this.ruleStore
            .getData(requestContext, updateRequest.getId())
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    ConfigObject<ServiceNamingRule> updatedRule =
        this.ruleStore.upsertObject(
            requestContext, this.ruleBuilder.buildUpdatedRule(existingRule, updateRequest));
    return this.ruleBuilder.buildFromConfigObject(updatedRule);
  }

  @Override
  public ServiceNamingRule createRule(
      RequestContext requestContext, CreateServiceNamingRuleRequest createRequest) {
    this.validator.validateOrThrow(requestContext, createRequest);
    ConfigObject<ServiceNamingRule> newRule =
        this.ruleStore.upsertObject(
            requestContext,
            this.ruleBuilder.buildNewRule(
                this.ruleStore.getAllConfigData(requestContext), createRequest));

    return this.ruleBuilder.buildFromConfigObject(newRule);
  }
}
