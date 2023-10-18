package ai.traceable.blocking.config.service.v1.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultIpTypeBlockingManager implements IpTypeBlockingManager {
  private final IpTypeRuleAggregatorBase<IpTypeRule> ipTypeRuleAggregator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultIpTypeBlockingManager(
      IpTypeRuleAggregatorBase<IpTypeRule> ipTypeRuleAggregator, UuidGenerator uuidGenerator) {
    this.ipTypeRuleAggregator = ipTypeRuleAggregator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public IpTypeBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {

    List<ImmutablePair<IpTypeRule, String>> ipTypeBlockingRules =
        ipTypeRuleAggregator.getEnabledBlockingRules(requestContext, environmentId);

    // Calculating uuid of IpTypeBlob is very costly due to large size of the blob, hence using the
    // existing hash
    String responseHash =
        uuidGenerator.generateId(
            ipTypeBlockingRules.stream()
                .map(ImmutablePair::getRight)
                .collect(Collectors.toUnmodifiableList()));

    IpTypeBlockingRules.Builder ipTypeBlockingRulesBuilder =
        IpTypeBlockingRules.newBuilder().setHash(responseHash);

    if (!responseHash.equals(requestHash)) {
      ipTypeBlockingRulesBuilder.addAllIpTypeRuleList(
          ipTypeBlockingRules.stream()
              .map(ImmutablePair::getLeft)
              .collect(Collectors.toUnmodifiableList()));
    }
    return ipTypeBlockingRulesBuilder.build();
  }
}
