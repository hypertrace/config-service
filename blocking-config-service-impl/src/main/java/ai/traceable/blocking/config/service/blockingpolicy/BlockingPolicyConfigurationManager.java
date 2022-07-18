package ai.traceable.blocking.config.service.blockingpolicy;

import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;

public interface BlockingPolicyConfigurationManager {
  BlockingPolicyConfiguration getBlockingPolicyConfiguration(String requestHash);
}
