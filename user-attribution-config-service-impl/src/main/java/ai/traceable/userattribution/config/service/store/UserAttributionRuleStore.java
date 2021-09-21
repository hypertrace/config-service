package ai.traceable.userattribution.config.service.store;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
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
public class UserAttributionRuleStore extends IdentifiedObjectStore<UserAttributionRule> {
  private static final String USER_ATTRIBUTION_RULE_RESOURCE_NAME = "user-attribution-rule";
  private static final String USER_ATTRIBUTION_RESOURCE_NAMESPACE = "user-attribution";

  private final RankCalculator<UserAttributionRule, String> rankCalculator;

  @Inject
  public UserAttributionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      RankCalculator<UserAttributionRule, String> rankCalculator) {
    super(
        configServiceBlockingStub,
        USER_ATTRIBUTION_RESOURCE_NAMESPACE,
        USER_ATTRIBUTION_RULE_RESOURCE_NAME);
    this.rankCalculator = rankCalculator;
  }

  @Override
  protected Optional<UserAttributionRule> buildObjectFromValue(Value value) {
    UserAttributionRule.Builder builder = UserAttributionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to UserAttributionRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromObject(UserAttributionRule object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromObject(UserAttributionRule object) {
    return object.getId();
  }

  @Override
  protected List<UserAttributionRule> orderFetchedObjects(List<UserAttributionRule> objects) {
    return this.rankCalculator.orderFromRanks(objects);
  }
}
