package ai.traceable.bot.categorized.policy.service.v1.store;

import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy.Builder;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class CategorizedBotConfigPolicyStore
    extends IdentifiedObjectStoreWithFilter<
        CategorizedBotConfigPolicy, CategorizedBotConfigPolicyFilter> {

  private static final String CATEGORIZED_BOT_CONFIG_POLICY_RESOURCE_NAME =
      "categorized_bot_config_policy_resource";
  private static final String CATEGORIZED_BOT_CONFIG_POLICY_RESOURCE_NAMESPACE =
      "categorized_bot_config_policy_namespace";

  @Inject
  public CategorizedBotConfigPolicyStore(
      final ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        CATEGORIZED_BOT_CONFIG_POLICY_RESOURCE_NAMESPACE,
        CATEGORIZED_BOT_CONFIG_POLICY_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<CategorizedBotConfigPolicy> filterConfigData(
      final CategorizedBotConfigPolicy data, final CategorizedBotConfigPolicyFilter filter) {
    return Optional.of(data)
        .filter(
            policy ->
                Optional.ofNullable(filter)
                    .map(CategorizedBotConfigPolicyFilter::getBotConfigPolicyIdsList)
                    .filter(ids -> !ids.isEmpty())
                    .map(ids -> ids.contains(policy.getId()))
                    .orElse(true));
  }

  @Override
  @SneakyThrows
  protected Optional<CategorizedBotConfigPolicy> buildDataFromValue(final Value value) {
    final Builder builder = CategorizedBotConfigPolicy.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(final CategorizedBotConfigPolicy data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(final CategorizedBotConfigPolicy data) {
    return data.getId();
  }
}
