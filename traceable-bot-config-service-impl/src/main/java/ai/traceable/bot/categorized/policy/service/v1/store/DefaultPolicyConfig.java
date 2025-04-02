package ai.traceable.bot.categorized.policy.service.v1.store;

import static java.util.function.Function.identity;

import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;

public class DefaultPolicyConfig {

  private static final String BOT_CONFIG_SERVICE = "bot.config.service";
  private static final String DEFAULT_POLICIES = "default.policies";
  private final Map<String, CategorizedBotConfigPolicy> defaultPolicies;

  @Inject
  public DefaultPolicyConfig(Config config) {
    final Config botConfig = config.getConfig(BOT_CONFIG_SERVICE);
    final List<? extends ConfigObject> defaultPoliciesConfigs =
        botConfig.getObjectList(DEFAULT_POLICIES);
    defaultPolicies =
        defaultPoliciesConfigs.stream()
            .map(DefaultPolicyConfig::buildPolicyFromConfig)
            .collect(ImmutableMap.toImmutableMap(CategorizedBotConfigPolicy::getId, identity()));
  }

  public boolean isDefaultPolicy(String id) {
    return defaultPolicies.containsKey(id);
  }

  public Optional<CategorizedBotConfigPolicy> getDefaultPolicy(String id) {
    return Optional.ofNullable(defaultPolicies.get(id));
  }

  public Collection<CategorizedBotConfigPolicy> getDefaultPolicies() {
    return defaultPolicies.values();
  }

  @SneakyThrows
  private static CategorizedBotConfigPolicy buildPolicyFromConfig(ConfigObject configObject) {
    final String jsonString = configObject.render();
    final CategorizedBotConfigPolicy.Builder builder = CategorizedBotConfigPolicy.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
