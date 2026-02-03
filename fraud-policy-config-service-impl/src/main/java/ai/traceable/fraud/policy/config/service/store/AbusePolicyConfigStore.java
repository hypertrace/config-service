package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyFilter;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePoliciesRequest;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AbusePolicyConfigStore
    extends IdentifiedObjectStoreWithFilter<AbusePolicy, GetAbusePoliciesRequest> {

  private static final String ABUSE_POLICY_RESOURCE_NAME = "abuse-policy";
  private static final String ABUSE_POLICY_CONFIG_RESOURCE_NAMESPACE = "abuse-policy-config";

  @Inject
  public AbusePolicyConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        ABUSE_POLICY_CONFIG_RESOURCE_NAMESPACE,
        ABUSE_POLICY_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<AbusePolicy> buildDataFromValue(Value value) {
    AbusePolicy.Builder abusePolicyBuilder = AbusePolicy.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, abusePolicyBuilder);
    return Optional.of(abusePolicyBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AbusePolicy abusePolicy) {
    return ConfigProtoConverter.convertToValue(abusePolicy);
  }

  @Override
  protected String getContextFromData(AbusePolicy abusePolicy) {
    return abusePolicy.getId();
  }

  @Override
  protected Optional<AbusePolicy> filterConfigData(
      AbusePolicy data, GetAbusePoliciesRequest request) {
    if (!request.hasFilter()) {
      return Optional.of(data);
    }

    AbusePolicyFilter filter = request.getFilter();

    return Optional.of(data)
        .filter(policy -> filterByEnabled(policy, filter))
        .filter(policy -> filterByPolicyIds(policy, filter))
        .filter(policy -> filterByEnvironmentIds(policy, filter))
        .filter(policy -> filterBySeverities(policy, filter))
        .filter(policy -> filterByApiIds(policy, filter))
        .filter(policy -> filterByActionTypes(policy, filter));
  }

  @Override
  public List<AbusePolicy> getAllConfigData(
      RequestContext requestContext, GetAbusePoliciesRequest request) {
    List<AbusePolicy> abusePolicies = super.getAllConfigData(requestContext, request);
    return abusePolicies.stream()
        .filter(abusePolicy -> filterConfigData(abusePolicy, request).isPresent())
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean filterByEnabled(AbusePolicy policy, AbusePolicyFilter filter) {
    if (!filter.hasEnabled()) {
      return true;
    }
    return policy.getData().getEnabled() == filter.getEnabled();
  }

  private boolean filterByPolicyIds(AbusePolicy policy, AbusePolicyFilter filter) {
    if (filter.getPolicyIdsCount() == 0) {
      return true;
    }
    return filter.getPolicyIdsList().contains(policy.getId());
  }

  private boolean filterByEnvironmentIds(AbusePolicy policy, AbusePolicyFilter filter) {
    if (filter.getEnvironmentIdsCount() == 0) {
      return true;
    }
    // Policy with no environment scope applies to all environments
    if (!policy.getData().getScope().hasEnvironmentScope()) {
      return true;
    }
    // Check if any environment ID matches
    return policy.getData().getScope().getEnvironmentScope().getEnvironmentIdsList().stream()
        .anyMatch(envId -> filter.getEnvironmentIdsList().contains(envId));
  }

  private boolean filterBySeverities(AbusePolicy policy, AbusePolicyFilter filter) {
    if (filter.getSeveritiesCount() == 0) {
      return true;
    }
    return filter.getSeveritiesList().contains(policy.getData().getSeverity());
  }

  private boolean filterByApiIds(AbusePolicy policy, AbusePolicyFilter filter) {
    if (filter.getApiIdsCount() == 0) {
      return true;
    }
    if (!policy.getData().getScope().getApiScope().hasApiIds()) {
      return false;
    }
    return policy.getData().getScope().getApiScope().getApiIds().getIdsList().stream()
        .anyMatch(apiId -> filter.getApiIdsList().contains(apiId));
  }

  private boolean filterByActionTypes(AbusePolicy policy, AbusePolicyFilter filter) {
    if (filter.getActionTypesCount() == 0) {
      return true;
    }
    return filter.getActionTypesList().contains(policy.getData().getAction().getActionType());
  }
}
