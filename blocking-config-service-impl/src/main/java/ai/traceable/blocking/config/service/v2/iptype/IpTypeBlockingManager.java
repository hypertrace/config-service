package ai.traceable.blocking.config.service.v2.iptype;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IpTypeBlockingManager implements BlockingConfigManagerBase {
  private final IpTypeRuleConverter ipTypeRuleConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public IpTypeBlockingManager(
      IpTypeRuleConverter ipTypeRuleConverter, UuidGenerator uuidGenerator) {
    this.ipTypeRuleConverter = ipTypeRuleConverter;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {
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
                blockingRulesSupplier.getIpTypeIpMappings(ipTypeRuleConverter::convert))
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
          .addAllAgentCapabilities(
              requestElements.stream()
                  .map(BlockingConfigRequestElement::getSupportedAgentCapabilitiesList)
                  .flatMap(List::stream)
                  .collect(Collectors.toUnmodifiableList()))
          .build();
    }
    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .setIpTypeBlockingRules(ipTypeBlockingRules)
        .addAllAgentCapabilities(
            requestElements.stream()
                .map(BlockingConfigRequestElement::getSupportedAgentCapabilitiesList)
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }
}
