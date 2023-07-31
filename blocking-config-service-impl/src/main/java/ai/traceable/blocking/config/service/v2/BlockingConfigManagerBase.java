package ai.traceable.blocking.config.service.v2;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import java.util.List;

public interface BlockingConfigManagerBase {

  List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier);
}
