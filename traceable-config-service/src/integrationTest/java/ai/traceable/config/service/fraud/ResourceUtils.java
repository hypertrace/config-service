package ai.traceable.config.service.fraud;

import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ResourceUtils {
  private static final JsonFormat.Parser jsonParser = JsonFormat.parser();

  public static List<String> getConfigs(List<String> files) throws IOException {
    List<String> configs = new ArrayList<>(files.size());
    for (var configFile : files) {
      URL resource = Resources.getResource(configFile);
      String yamlConfig = Resources.toString(resource, Charset.defaultCharset());
      configs.add(yamlConfig);
    }
    return configs;
  }

  public static String getConfig(String file) throws IOException {
    URL resource = Resources.getResource(file);
    return Resources.toString(resource, Charset.defaultCharset());
  }

  public static <T extends Message.Builder> T readProto(String resourceName, T builder)
      throws IOException {
    var url = Resources.getResource(resourceName);
    String data = Resources.toString(url, Charset.defaultCharset());
    return deserialize(data, builder);
  }

  public static <T extends Message.Builder> T deserialize(String value, T builder) {
    try {
      jsonParser.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
