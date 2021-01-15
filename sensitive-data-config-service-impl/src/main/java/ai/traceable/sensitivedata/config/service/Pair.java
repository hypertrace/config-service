package ai.traceable.sensitivedata.config.service;

import lombok.Value;

@Value(staticConstructor = "of")
public class Pair<K, V> {
  K key;
  V value;
}
