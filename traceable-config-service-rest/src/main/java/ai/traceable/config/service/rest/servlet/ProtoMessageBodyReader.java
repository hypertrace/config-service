package ai.traceable.config.service.rest.servlet;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyReader;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import lombok.SneakyThrows;

@Provider
public class ProtoMessageBodyReader implements MessageBodyReader<Message> {

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private final LoadingCache<Class<? extends Message>, Message> messagePrototypeLookup =
      CacheBuilder.newBuilder().maximumSize(1000).build(CacheLoader.from(this::lookupPrototype));

  @Override
  public boolean isReadable(
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return Message.class.isAssignableFrom(type);
  }

  @Override
  public Message readFrom(
      Class<Message> type,
      Type genericType,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders,
      InputStream entityStream)
      throws IOException {
    Message.Builder builder = this.messagePrototypeLookup.getUnchecked(type).newBuilderForType();
    try {
      JSON_PARSER.merge(new InputStreamReader(entityStream), builder);
      return builder.build();
    } catch (Exception e) {
      return builder.build();
    }
  }

  @SneakyThrows
  private Message lookupPrototype(Class<? extends Message> protoClass) {
    return (Message) protoClass.getMethod("getDefaultInstance").invoke(null);
  }
}
