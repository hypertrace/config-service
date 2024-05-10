package org.hypertrace.config.validation;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class GrpcValidatorUtils {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static final IPAddressStringParameters ADDRESS_VALIDATION_PARAMS =
      new IPAddressStringParameters.Builder()
          // Allows ipv4 joined segments like 1.2.3, 1.2, or just 1 For the case of just 1 segment
          .allow_inet_aton(false)
          // Allows an address to be specified as a single value, eg ffffffff, without the standard
          // use of segments like 1.2.3.4 or 1:2:4:3:5:6:7:8
          .allowSingleSegment(false)
          // Allows zero-length IPAddressStrings like ""
          .allowEmpty(false)
          .toParams();

  private GrpcValidatorUtils() {}

  public static void validateRequestContextOrThrow(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException();
    }
  }

  public static <T extends Message> void validateNonDefaultPresenceOrThrow(
      T source, int fieldNumber) {
    FieldDescriptor descriptor = source.getDescriptorForType().findFieldByNumber(fieldNumber);

    if (descriptor.isRepeated()) {
      validateNonDefaultPresenceRepeatedOrThrow(source, descriptor);
    } else if (!source.hasField(descriptor)
        || source.getField(descriptor).equals(descriptor.getDefaultValue())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected field value %s but not present:%n %s",
                  descriptor.getFullName(), printMessage(source)))
          .asRuntimeException();
    }
  }

  public static boolean isValidIpAddressOrSubnet(final String input) {
    return new IPAddressString(input, ADDRESS_VALIDATION_PARAMS).getAddress() != null;
  }

  /**
   * As opposed to `validateNonDefaultPresenceOrThrow` which looks for a non default value, here
   * defaults are allowed as long as the field has been explicitly assigned
   */
  public static <T extends Message> void validateFieldPresenceOrThrow(T source, int fieldNumber) {
    FieldDescriptor descriptor = source.getDescriptorForType().findFieldByNumber(fieldNumber);
    if (!descriptor.hasPresence()) {
      throw Status.INTERNAL
          .withDescription(
              String.format(
                  "Improper use of 'validateOptionalFieldAssignedOrThrow' field without presence: %s",
                  descriptor.getFullName()))
          .asRuntimeException();
    }
    if (!source.hasField(descriptor)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected field to be assigned:%n %s",
                  descriptor.getFullName(), printMessage(source)))
          .asRuntimeException();
    }
  }

  private static <T extends Message> void validateNonDefaultPresenceRepeatedOrThrow(
      T source, FieldDescriptor descriptor) {
    if (source.getRepeatedFieldCount(descriptor) == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected at least 1 value for repeated field %s but not present:%n %s",
                  descriptor.getFullName(), printMessage(source)))
          .asRuntimeException();
    }
  }

  public static String printMessage(Message message) {
    try {
      return JSON_PRINTER.print(message);
    } catch (Exception exception) {
      return message.toString();
    }
  }
}
