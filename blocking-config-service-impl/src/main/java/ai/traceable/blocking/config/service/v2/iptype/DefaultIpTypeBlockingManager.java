package ai.traceable.blocking.config.service.v2.iptype;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DefaultIpTypeBlockingManager implements IpTypeBlockingManager {
  private final IpTypeRuleAggregatorBase<IpTypeRule> ipTypeRuleAggregator;

  @Inject
  public DefaultIpTypeBlockingManager(IpTypeRuleAggregatorBase<IpTypeRule> ipTypeRuleAggregator) {
    this.ipTypeRuleAggregator = ipTypeRuleAggregator;
  }

  @Override
  public IpTypeBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    return IpTypeBlockingRules.newBuilder()
        .addAllIpTypeRuleList(
            ipTypeRuleAggregator.getEnabledBlockingRules(requestContext, environmentId))
        .build();
  }
}
