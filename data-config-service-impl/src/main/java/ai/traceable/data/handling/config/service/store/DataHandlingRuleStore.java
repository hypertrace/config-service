package ai.traceable.data.handling.config.service.store;

import ai.traceable.data.handling.config.service.utils.DataHandlingRuleRankCalculator;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
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
public class DataHandlingRuleStore {
  private static final String DATA_HANDLING_RULE_RESOURCE_NAME = "data-handling-rule";
  private static final String DATA_HANDLING_RESOURCE_NAMESPACE = "data-handling";
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final DataHandlingRuleRankCalculator rankCalculator;

  @Inject
  public DataHandlingRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      DataHandlingRuleRankCalculator rankCalculator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.rankCalculator = rankCalculator;
  }

  public List<DataHandlingRule> getRules(RequestContext context) {
    return context
        .call(
            () ->
                this.configServiceBlockingStub.getAllConfigs(
                    GetAllConfigsRequest.newBuilder()
                        .setResourceName(DATA_HANDLING_RULE_RESOURCE_NAME)
                        .setResourceNamespace(DATA_HANDLING_RESOURCE_NAMESPACE)
                        .build()))
        .getContextSpecificConfigsList()
        .stream()
        .map(ContextSpecificConfig::getConfig)
        .map(this::buildRule)
        .flatMap(Optional::stream)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toUnmodifiableList(), this.rankCalculator::orderFromRanks));
  }

  public Optional<DataHandlingRule> getRule(RequestContext context, String id) {
    try {
      Value value =
          context.call(
              () ->
                  this.configServiceBlockingStub
                      .getConfig(
                          GetConfigRequest.newBuilder()
                              .setResourceName(DATA_HANDLING_RULE_RESOURCE_NAME)
                              .setResourceNamespace(DATA_HANDLING_RESOURCE_NAMESPACE)
                              .addContexts(id)
                              .build())
                      .getConfig());
      DataHandlingRule rule =
          this.buildRule(value).orElseThrow(Status.INTERNAL::asRuntimeException);
      return Optional.of(rule);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public DataHandlingRule upsertRule(RequestContext context, DataHandlingRule rule) {
    Value upsertedValue =
        context.call(
            () ->
                this.configServiceBlockingStub
                    .upsertConfig(
                        UpsertConfigRequest.newBuilder()
                            .setResourceName(DATA_HANDLING_RULE_RESOURCE_NAME)
                            .setResourceNamespace(DATA_HANDLING_RESOURCE_NAMESPACE)
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
                    .setResourceName(DATA_HANDLING_RULE_RESOURCE_NAME)
                    .setResourceNamespace(DATA_HANDLING_RESOURCE_NAMESPACE)
                    .setContext(id)
                    .build()));
  }

  public List<DataHandlingRule> upsertAllRules(
      RequestContext context, List<DataHandlingRule> rules) {
    // TODO push down a bulk upsert API into generic service
    return rules.stream()
        .map(rule -> this.upsertRule(context, rule))
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<DataHandlingRule> buildRule(Value value) {
    DataHandlingRule.Builder builder = DataHandlingRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to DataHandlingRule: {}", value, e);
      return Optional.empty();
    }
  }
}
