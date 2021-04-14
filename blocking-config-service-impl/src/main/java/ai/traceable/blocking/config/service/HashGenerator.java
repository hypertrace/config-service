package ai.traceable.blocking.config.service;

import ai.traceable.blocking.config.service.v1.BlockingRules;

class HashGenerator {
  String generate(BlockingRules rules) {
    return String.valueOf(rules.hashCode());
  }
}
