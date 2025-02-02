package ai.traceable.config.proto.utils;

import static ai.traceable.config.proto.utils.ProtoUtils.deserialize;
import static ai.traceable.config.proto.utils.ProtoUtils.deserializeYaml;

import com.google.common.io.Resources;
import com.google.protobuf.Message;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ResourceUtils {
  public static List<String> getConfigs(List<String> files) throws IOException {
    List<String> configs = new ArrayList<>(files.size());
    for (String configFile : files) {
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
    URL url = Resources.getResource(resourceName);
    String data = Resources.toString(url, Charset.defaultCharset());
    return deserialize(data, builder);
  }

  public static <T extends Message.Builder> T readProtoFromYaml(String resourceName, T builder)
      throws IOException {
    URL url = Resources.getResource(resourceName);
    String data = Resources.toString(url, Charset.defaultCharset());
    return deserializeYaml(data, builder);
  }

  public static List<String> getResourceFiles(Class klass, String dir) throws IOException {
    List<String> filenames = new ArrayList<>();

    try (InputStream in = klass.getResourceAsStream(dir);
        BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
      String resource;
      String prefix = dir.substring(1);
      while ((resource = br.readLine()) != null) {
        filenames.add(String.format("%s/%s", prefix, resource));
      }
    }

    return filenames;
  }

  public static List<String> getConfigs(Class klass, String dir, String namePrefix)
      throws IOException {
    List<String> configFiles = getResourceFiles(klass, dir);
    List<String> configs = new ArrayList<>(configFiles.size());
    for (String configFile : configFiles) {
      URL resource = Resources.getResource(configFile);
      String yamlConfig = Resources.toString(resource, Charset.defaultCharset());
      configs.add(yamlConfig);
    }
    return configs;
  }

  public static Config getConf(String resourceName) {
    return ConfigFactory.parseFile(
            new File(
                Objects.requireNonNull(
                        Thread.currentThread().getContextClassLoader().getResource(resourceName))
                    .getPath()))
        .resolve();
  }
}
