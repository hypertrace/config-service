package org.hypertrace.config.service;

import static org.hypertrace.config.proto.converter.ConfigProtoConverter.mergeFromJsonString;

import com.google.common.io.Resources;
import com.google.protobuf.Value;
import java.io.IOException;
import java.nio.charset.Charset;

public class IntegrationTestUtils {

  /**
   * Reads the Json file and converts it into {@link Value} object
   *
   * @param jsonFileName
   * @return
   * @throws IOException
   */
  public static Value getConfigValue(String jsonFileName) throws IOException {
    String jsonConfigString = getConfigString(jsonFileName);
    Value.Builder valueBuilder = Value.newBuilder();
    mergeFromJsonString(jsonConfigString, valueBuilder);
    return valueBuilder.build();
  }

  private static String getConfigString(String resourceName) throws IOException {
    return Resources.toString(Resources.getResource(resourceName), Charset.defaultCharset());
  }
}
