package ai.traceable.blocking.config.service.v2.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class IpTypeBlockingManager implements BlockingConfigManagerBase {
  private final IpTypeRuleAggregatorBase<IpTypeRule> ipTypeRuleAggregator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public IpTypeBlockingManager(
      IpTypeRuleAggregatorBase<IpTypeRule> ipTypeRuleAggregator, UuidGenerator uuidGenerator) {
    this.ipTypeRuleAggregator = ipTypeRuleAggregator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      RequestContext requestContext,
      Optional<String> environmentId) {
    List<BlockingConfigRequestElement> ipTypeRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasIpTypeBlockingRulesRequest)
            .collect(Collectors.toUnmodifiableList());
    if (ipTypeRequestElements.isEmpty()) {
      return Collections.emptyList();
    }

    IpTypeBlockingRules ipTypeBlockingRules =
        IpTypeBlockingRules.newBuilder()
            .addAllIpTypeRuleList(
                ipTypeRuleAggregator.getEnabledBlockingRules(requestContext, environmentId))
            .build();
    return List.of(buildResponseElement(ipTypeRequestElements, ipTypeBlockingRules));
  }

  private BlockingConfigResponseElement buildResponseElement(
      List<BlockingConfigRequestElement> requestElements, IpTypeBlockingRules ipTypeBlockingRules) {
    String responseHash = uuidGenerator.generateId(ipTypeBlockingRules);
    if (requestElements.stream()
        .map(BlockingConfigRequestElement::getPreviousHash)
        .allMatch(responseHash::equals)) {
      return BlockingConfigResponseElement.newBuilder()
          .setHash(responseHash)
          .setIpTypeBlockingRules(IpTypeBlockingRules.getDefaultInstance())
          .build();
    }
    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .setIpTypeBlockingRules(ipTypeBlockingRules)
        .build();
  }
}
