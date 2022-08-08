package org.hypertrace.config.service;

import static org.hypertrace.config.service.TestUtils.getConfig1;
import static org.hypertrace.config.service.TestUtils.getConfig2;
import static org.hypertrace.config.service.TestUtils.getExpectedMergedConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.junit.jupiter.api.Test;

class ConfigServiceUtilsTest {

  @Test
  void merge() {
    Value config1 = getConfig1();
    Value config2 = getConfig2();
    ContextSpecificConfig contextSpecificConfig1 =
        ContextSpecificConfig.newBuilder()
            .setConfig(config1)
            .setCreationTimestamp(10L)
            .setUpdateTimestamp(12L)
            .build();
    ContextSpecificConfig contextSpecificConfig2 =
        ContextSpecificConfig.newBuilder()
            .setConfig(config2)
            .setCreationTimestamp(15L)
            .setUpdateTimestamp(25L)
            .build();

    // test merging 2 config values
    ContextSpecificConfig mergedContextSpecificConfig =
        ConfigServiceUtils.merge(contextSpecificConfig1, contextSpecificConfig2);
    assertEquals(getExpectedMergedConfig(), mergedContextSpecificConfig.getConfig());
    assertEquals(15L, mergedContextSpecificConfig.getCreationTimestamp());
    assertEquals(25L, mergedContextSpecificConfig.getUpdateTimestamp());

    // test merging with null value
    Value nullConfigValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    ContextSpecificConfig nullContextSpecificConfig =
        ContextSpecificConfig.newBuilder().setConfig(nullConfigValue).build();

    mergedContextSpecificConfig =
        ConfigServiceUtils.merge(contextSpecificConfig1, nullContextSpecificConfig);
    assertEquals(config1, mergedContextSpecificConfig.getConfig());
    assertEquals(10L, mergedContextSpecificConfig.getCreationTimestamp());
    assertEquals(12L, mergedContextSpecificConfig.getUpdateTimestamp());

    mergedContextSpecificConfig =
        ConfigServiceUtils.merge(nullContextSpecificConfig, contextSpecificConfig1);
    assertEquals(config1, mergedContextSpecificConfig.getConfig());
    assertEquals(10L, mergedContextSpecificConfig.getCreationTimestamp());
    assertEquals(12L, mergedContextSpecificConfig.getUpdateTimestamp());
  }
}
