package ai.traceable.data.handling.config.service.store;

import ai.traceable.data.handling.config.service.utils.DataHandlingRuleRankCalculator;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DataHandlingRuleStore extends IdentifiedObjectStore<DataHandlingRule> {
  private static final String DATA_HANDLING_RULE_RESOURCE_NAME = "data-handling-rule";
  private static final String DATA_HANDLING_RESOURCE_NAMESPACE = "data-handling";
  private final DataHandlingRuleRankCalculator rankCalculator;

  @Inject
  public DataHandlingRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      DataHandlingRuleRankCalculator rankCalculator) {
    super(
        configServiceBlockingStub,
        DATA_HANDLING_RESOURCE_NAMESPACE,
        DATA_HANDLING_RULE_RESOURCE_NAME);
    this.rankCalculator = rankCalculator;
  }

  public List<DataHandlingRule> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected Optional<DataHandlingRule> buildDataFromValue(Value value) {
    DataHandlingRule.Builder builder = DataHandlingRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to DataHandlingRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DataHandlingRule data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(DataHandlingRule rule) {
    return rule.getId();
  }

  @Override
  protected List<ContextualConfigObject<DataHandlingRule>> orderFetchedObjects(
      List<ContextualConfigObject<DataHandlingRule>> objects) {
    return this.rankCalculator.orderFromRanks(objects, ContextualConfigObject::getData);
  }
}
