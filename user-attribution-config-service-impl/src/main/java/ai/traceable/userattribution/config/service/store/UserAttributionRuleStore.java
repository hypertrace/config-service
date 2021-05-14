package ai.traceable.userattribution.config.service.store;

import static ai.traceable.userattribution.config.service.store.UserAttributionRuleRankCalculator.RULE_RANK_COMPARATOR;

import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class UserAttributionRuleStore {
  private static final String USER_ATTRIBUTION_RULE_RESOURCE_NAME = "user-attribution-rule";
  private static final String USER_ATTRIBUTION_RESOURCE_NAMESPACE = "user-attribution";

  private final ConfigServiceBlockingStub configServiceBlockingStub;

  @Inject
  public UserAttributionRuleStore(ConfigServiceBlockingStub configServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  public List<UserAttributionRule> getRules(RequestContext context) {
    return context
        .call(
            () ->
                this.configServiceBlockingStub.getAllConfigs(
                    GetAllConfigsRequest.newBuilder()
                        .setResourceName(USER_ATTRIBUTION_RULE_RESOURCE_NAME)
                        .setResourceNamespace(USER_ATTRIBUTION_RESOURCE_NAMESPACE)
                        .build()))
        .getContextSpecificConfigsList()
        .stream()
        .map(ContextSpecificConfig::getConfig)
        .map(this::buildRule)
        .flatMap(Optional::stream)
        .sorted(RULE_RANK_COMPARATOR)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<UserAttributionRule> getRule(RequestContext context, String id) {
    try {
      Value value =
          context.call(
              () ->
                  this.configServiceBlockingStub
                      .getConfig(
                          GetConfigRequest.newBuilder()
                              .setResourceName(USER_ATTRIBUTION_RULE_RESOURCE_NAME)
                              .setResourceNamespace(USER_ATTRIBUTION_RESOURCE_NAMESPACE)
                              .addContexts(id)
                              .build())
                      .getConfig());
      UserAttributionRule rule =
          this.buildRule(value).orElseThrow(Status.INTERNAL::asRuntimeException);
      return Optional.of(rule);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public UserAttributionRule upsertRule(RequestContext context, UserAttributionRule rule) {
    Value upsertedValue =
        context.call(
            () ->
                this.configServiceBlockingStub
                    .upsertConfig(
                        UpsertConfigRequest.newBuilder()
                            .setResourceName(USER_ATTRIBUTION_RULE_RESOURCE_NAME)
                            .setResourceNamespace(USER_ATTRIBUTION_RESOURCE_NAMESPACE)
                            .setContext(rule.getId())
                            .setConfig(ConfigProtoConverter.convertToValue(rule))
                            .build())
                    .getConfig());

    return this.buildRule(upsertedValue).orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  public void deleteRule(RequestContext context, String id) {
    context.call(
        () ->
            this.configServiceBlockingStub.deleteConfig(
                DeleteConfigRequest.newBuilder()
                    .setResourceName(USER_ATTRIBUTION_RULE_RESOURCE_NAME)
                    .setResourceNamespace(USER_ATTRIBUTION_RESOURCE_NAMESPACE)
                    .setContext(id)
                    .build()));
  }

  public List<UserAttributionRule> upsertAllRules(
      RequestContext context, List<UserAttributionRule> rules) {
    // TODO push down a bulk upsert API into generic service
    return rules.stream()
        .map(rule -> this.upsertRule(context, rule))
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<UserAttributionRule> buildRule(Value value) {
    UserAttributionRule.Builder builder = UserAttributionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to UserAttributionRule: {}", value, e);
      return Optional.empty();
    }
  }
}
