package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.v1.BlockingRule;
import java.util.List;

class HashGenerator {
  String generate(List<BlockingRule> rules) {
    return String.valueOf(rules.hashCode());
  }
}
