package ai.traceable.data.handling.config.service.store;

import ai.traceable.data.handling.config.service.utils.DataHandlingRuleRankCalculator;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

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

  @Override
  protected Optional<DataHandlingRule> buildObjectFromValue(Value value) {
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
  protected Value buildValueFromObject(DataHandlingRule object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromObject(DataHandlingRule rule) {
    return rule.getId();
  }

  @Override
  protected List<DataHandlingRule> orderFetchedObjects(List<DataHandlingRule> objects) {
    return this.rankCalculator.orderFromRanks(objects);
  }
}
