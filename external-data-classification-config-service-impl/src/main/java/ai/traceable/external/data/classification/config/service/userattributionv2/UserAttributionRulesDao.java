package ai.traceable.external.data.classification.config.service.userattributionv2;

import static ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.UserAttributionRuleSource.USER_ATTRIBUTION_RULE_SOURCE_V2;

import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.EnvironmentFilter;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class UserAttributionRulesDao {
  private final UserAttributionConfigServiceBlockingStub userAttributionConfigServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  public UserAttributionRulesDao(
      UserAttributionConfigServiceBlockingStub userAttributionConfigServiceBlockingStub,
      ClientConfig clientConfig) {
    this.userAttributionConfigServiceBlockingStub = userAttributionConfigServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  public List<UserAttributionRule> getEnabledUserAttributionRules(
      RequestContext requestContext, EnvironmentFilter environmentFilter) {
    List<String> matchingEnvironments =
        Optional.of(environmentFilter.getEnvironmentName())
            .filter(Predicate.not(String::isBlank))
            .map(List::of)
            .orElseGet(Collections::emptyList); // no matching envs means rules with no scoped env
    return requestContext.call(
        () ->
            userAttributionConfigServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getUserAttributionRules(
                    GetUserAttributionRulesRequest.newBuilder()
                        .setRuleSource(USER_ATTRIBUTION_RULE_SOURCE_V2)
                        .setFilter(
                            GetUserAttributionRulesFilter.newBuilder()
                                .setDisabled(false)
                                .setEnvironmentFilter(
                                    GetUserAttributionRulesFilter.EnvironmentFilter.newBuilder()
                                        .addAllEnvironmentNames(matchingEnvironments)))
                        .build())
                .getRulesList()
                .stream()
                .collect(Collectors.toUnmodifiableList()));
  }
}
