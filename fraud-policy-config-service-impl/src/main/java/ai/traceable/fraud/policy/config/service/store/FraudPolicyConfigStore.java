package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudPolicyConfigStore
    extends IdentifiedObjectStoreWithFilter<FraudPolicy, GetFraudPolicyListRequest> {

  private static final String FRAUD_POLICY_RESOURCE_NAME = "fraud-policy";
  private static final String FRAUD_POLICY_CONFIG_RESOURCE_NAMESPACE = "fraud-policy-config";

  @Inject
  public FraudPolicyConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        FRAUD_POLICY_CONFIG_RESOURCE_NAMESPACE,
        FRAUD_POLICY_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<FraudPolicy> buildDataFromValue(Value value) {
    FraudPolicy.Builder fraudPolicyBuilder = FraudPolicy.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, fraudPolicyBuilder);
    return Optional.of(fraudPolicyBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(FraudPolicy fraudPolicy) {
    return ConfigProtoConverter.convertToValue(fraudPolicy);
  }

  @Override
  protected String getContextFromData(FraudPolicy fraudPolicy) {
    return fraudPolicy.getId();
  }

  @Override
  protected Optional<FraudPolicy> filterConfigData(
      FraudPolicy data, GetFraudPolicyListRequest request) {
    // check if request has any ids specified
    return Optional.of(data)
        .filter(policy -> !request.getIncludeDisabled() && !policy.getDisabled())
        .filter(
            policy ->
                request.getFraudPolicyIdCount() == 0
                    || request.getFraudPolicyIdList().contains(data.getId()));
  }

  @Override
  public List<FraudPolicy> getAllConfigData(
      RequestContext requestContext, GetFraudPolicyListRequest request) {
    List<FraudPolicy> fraudPolicies = super.getAllConfigData(requestContext, request);
    return fraudPolicies.stream()
        .filter(fraudPolicy -> filterConfigData(fraudPolicy, request).isPresent())
        .collect((Collectors.toUnmodifiableList()));
  }
}
