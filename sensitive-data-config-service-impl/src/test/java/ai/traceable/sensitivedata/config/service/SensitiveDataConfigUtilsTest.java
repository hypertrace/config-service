package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.TestUtils.getPiiFilterConfigInstance;
import static ai.traceable.sensitivedata.config.service.TestUtils.getPiiFilterConfigValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

class SensitiveDataConfigUtilsTest {

  @Test
  void toValue() throws InvalidProtocolBufferException {
    Value expected = getPiiFilterConfigValue();
    Value actual = SensitiveDataConfigUtils.toValue(getPiiFilterConfigInstance());
    assertEquals(expected, actual);
  }

  @Test
  void toPiiFilterConfig() throws InvalidProtocolBufferException {
    PiiFilterConfig expected = getPiiFilterConfigInstance();
    PiiFilterConfig actual = SensitiveDataConfigUtils.toPiiFilterConfig(getPiiFilterConfigValue());
    assertEquals(expected, actual);
  }

}