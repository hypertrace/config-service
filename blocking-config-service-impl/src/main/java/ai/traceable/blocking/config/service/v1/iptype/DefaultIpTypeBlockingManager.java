package ai.traceable.blocking.config.service.v1.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultIpTypeBlockingManager implements IpTypeBlockingManager {
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

    List<IpTypeRule> ipTypeRules =
        ipTypeRuleAggregator.getEnabledBlockingRules(requestContext, environmentId);
    String responseHash = uuidGenerator.generateId(ipTypeRules);
    IpTypeBlockingRules.Builder ipTypeBlockingRulesBuilder =
        IpTypeBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      ipTypeBlockingRulesBuilder.addAllIpTypeRuleList(ipTypeRules);
    }
    return ipTypeBlockingRulesBuilder.build();
  }
}
