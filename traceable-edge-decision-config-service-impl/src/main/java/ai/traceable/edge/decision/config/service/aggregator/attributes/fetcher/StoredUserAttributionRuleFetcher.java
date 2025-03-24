package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import static org.hypertrace.config.objectstore.ClientConfig.DEFAULT;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.CachedUserAttributionJexlGenerator;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class StoredUserAttributionRuleFetcher implements UserAttributionRuleFetcher {
  private static final GetUserAttributionRulesRequest USER_ATTRIBUTION_RULES_REQUEST =
      GetUserAttributionRulesRequest.newBuilder()
          .setFilter(GetUserAttributionRulesFilter.newBuilder().setDisabled(false))
          .build();
  private final CachedUserAttributionJexlGenerator userAttributionJexlGenerator;
  private final UserAttributionConfigServiceBlockingStub configServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  public StoredUserAttributionRuleFetcher(
      CachedUserAttributionJexlGenerator userAttributionJexlGenerator,
      UserAttributionConfigServiceBlockingStub configServiceBlockingStub) {
    this.userAttributionJexlGenerator = userAttributionJexlGenerator;
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.clientConfig = DEFAULT;
  }

  @Override
  public List<DerivationRule> getUserAttributionRules(RequestContext requestContext) {
    GetUserAttributionRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getUserAttributionRules(USER_ATTRIBUTION_RULES_REQUEST));

    return response.getRulesList().stream()
        .map(UserAttributionRule::getData)
        .map(
            userAttributionRuleData ->
                userAttributionJexlGenerator.convert(requestContext, userAttributionRuleData))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }
}
