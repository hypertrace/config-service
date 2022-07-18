package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.v1.BlockingDetails;
import java.util.List;

public interface CustomSignatureDataFetcher {
  List<BlockingDetails> getCustomSignatureExemptions();

  List<BlockingDetails> getCustomSignatureViolations();
}
