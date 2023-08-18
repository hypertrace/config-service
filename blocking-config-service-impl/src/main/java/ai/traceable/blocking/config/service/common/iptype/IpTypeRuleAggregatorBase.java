package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class IpTypeRuleAggregatorBase<T> {
  Supplier<Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo>> ipTypeRulesSupplier;
  private final BlockingIpTypesClient blockingIpTypesClient;
  private final GenericIpTypeRuleConverter<T> ipTypeConverter;

  @Inject
  public IpTypeRuleAggregatorBase(
      IpTypeRulesLoader ipTypeRulesLoader,
      BlockingIpTypesClient blockingIpTypesClient,
      GenericIpTypeRuleConverter<T> ipTypeConverter) {
    this.ipTypeRulesSupplier = ipTypeRulesLoader.getLatestDataSupplier();
    this.blockingIpTypesClient = blockingIpTypesClient;
    this.ipTypeConverter = ipTypeConverter;
  }

  public List<T> getEnabledBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    List<MaliciousSourcesRule> maliciousSourcesRules =
        blockingIpTypesClient.fetchMaliciousSourceRules(requestContext, environmentId);

    List<IpLocationType> blockingIpTypes =
        maliciousSourcesRules.stream()
            .flatMap(
                maliciousSourcesRule ->
                    BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule).stream())
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo> ipTypesInfoMap = ipTypeRulesSupplier.get();
    return blockingIpTypes.stream()
        .map(IpTypeRuleInfo::convertIpType)
        .map(ipTypesInfoMap::get)
        .filter(Objects::nonNull)
        .map(ipTypeConverter::convert)
        .collect(Collectors.toUnmodifiableList());
  }

  public interface GenericIpTypeRuleConverter<T> {
    T convert(IpTypeRuleInfo ipTypeRule);
  }
}
