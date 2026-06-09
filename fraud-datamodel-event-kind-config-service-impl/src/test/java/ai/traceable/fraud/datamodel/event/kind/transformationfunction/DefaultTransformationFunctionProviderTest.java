package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunction;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DefaultTransformationFunctionProviderTest {

  private static final ComplexDataModelEventKind STRING_KIND =
      ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string").build();

  private DefaultTransformationFunctionProvider provider;

  @BeforeEach
  void setUp() {
    DefaultEventKindProvider eventKinds = new DefaultEventKindProvider();
    EventKindHierarchyResolver resolver = new EventKindHierarchyResolver(eventKinds);
    provider = new DefaultTransformationFunctionProvider(resolver, eventKinds);
  }

  @Test
  void getFunctionsByKinds_withStringKind_returnsExpectedFunctions() {
    List<TransformationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(STRING_KIND));

    assertFalse(result.isEmpty(), "Should return functions for string kind");

    List<String> functionIds =
        result.get(0).getFunctionsList().stream()
            .map(TransformationFunction::getId)
            .collect(Collectors.toList());

    assertTrue(functionIds.contains("system_defined_function_base64_decode"));
    assertTrue(
        functionIds.contains("type_cast_to_system_event_kind_email"),
        "Generated type cast uses id type_cast_to_<event_kind_id>");
    assertTrue(
        functionIds.contains("system_defined_function_regex_extract"),
        "regexExtract must be discoverable (was broken by event_kind_id typo)");
  }

  @Test
  void getFunctionsByKinds_withArrayKind_returnsGetFunction() {
    ComplexDataModelEventKind arrayKind =
        ComplexDataModelEventKind.newBuilder()
            .setArrayOf(
                ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_string"))
            .build();

    List<TransformationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(arrayKind));

    assertFalse(result.isEmpty());
    assertTrue(
        result.get(0).getFunctionsList().stream()
            .anyMatch(f -> f.getId().equals("system_defined_function_get")));
  }

  @Test
  void getAllFunctions_returnsNonEmptyResult() {
    List<TransformationFunctionsByKind> result = provider.getAllFunctions();

    assertFalse(result.isEmpty(), "Should return all function mappings");
    assertTrue(
        result.stream().allMatch(m -> !m.getFunctionsList().isEmpty()),
        "Each mapping should have at least one function");
  }

  @Test
  void getFunctionsByKinds_functionsDoNotContainOperators() {
    List<TransformationFunctionsByKind> functions =
        provider.getFunctionsByKinds(List.of(STRING_KIND));

    assertFalse(
        functions.get(0).getFunctionsList().stream().anyMatch(f -> f.getId().contains("operator")),
        "Functions list should not contain operators");
  }

  @Test
  void typeCastFunction_numericKind_usesToNum() {
    TransformationFunction integerCast = findFunction("type_cast_to_system_event_kind_integer");
    assertEquals("traceable:toNum(${input})", integerCast.getJexlTemplate());

    TransformationFunction floatCast = findFunction("type_cast_to_system_event_kind_float");
    assertEquals("traceable:toNum(${input})", floatCast.getJexlTemplate());

    TransformationFunction numericCast = findFunction("type_cast_to_system_event_kind_numeric");
    assertEquals("traceable:toNum(${input})", numericCast.getJexlTemplate());
  }

  @Test
  void typeCastFunction_stringKind_usesToStr() {
    TransformationFunction emailCast = findFunction("type_cast_to_system_event_kind_email");
    assertEquals("traceable:toStr(${input})", emailCast.getJexlTemplate());
  }

  @Test
  void typeCastFunction_boolAndTimestamp_usesToStr() {
    TransformationFunction boolCast = findFunction("type_cast_to_system_event_kind_boolean");
    assertEquals("traceable:toStr(${input})", boolCast.getJexlTemplate());

    TransformationFunction timestampCast = findFunction("type_cast_to_system_event_kind_timestamp");
    assertEquals("traceable:toStr(${input})", timestampCast.getJexlTemplate());
  }

  /**
   * Tests that type cast functions accept any input kind (system_event_kind_value) rather than
   * being restricted to string inputs. This validates the change from string-only to universal
   * input support and exercises the hierarchy-based compatibility check across every leaf kind.
   */
  @Nested
  class TypeCastAcceptsAnyInputKind {

    private final ComplexDataModelEventKind NUMERIC_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
    private final ComplexDataModelEventKind INTEGER_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_integer").build();
    private final ComplexDataModelEventKind FLOAT_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_float").build();
    private final ComplexDataModelEventKind BOOLEAN_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_boolean").build();
    private final ComplexDataModelEventKind TIMESTAMP_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_timestamp").build();
    private final ComplexDataModelEventKind EMAIL_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_email").build();
    private final ComplexDataModelEventKind IP_ADDRESS_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_ip_address").build();
    private final ComplexDataModelEventKind MAC_ADDRESS_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_mac_address").build();
    private final ComplexDataModelEventKind PHONE_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_phone_number").build();
    private final ComplexDataModelEventKind CREDIT_CARD_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_credit_card").build();
    private final ComplexDataModelEventKind VALUE_KIND =
        ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_value").build();

    @Test
    void typeCastInputKind_isValueKind() {
      TransformationFunction cast = findFunction("type_cast_to_system_event_kind_string");
      assertEquals(1, cast.getInputKindsCount());
      assertEquals("system_event_kind_value", cast.getInputKinds(0).getKindId());
    }

    @Test
    void numericKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(NUMERIC_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Numeric kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_email"),
          "Numeric kind should be castable to email");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_integer"),
          "Numeric kind should be castable to integer (self-cast)");
    }

    @Test
    void integerKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(INTEGER_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Integer kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_float"),
          "Integer kind should be castable to float");
    }

    @Test
    void floatKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(FLOAT_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Float kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_integer"),
          "Float kind should be castable to integer");
    }

    @Test
    void booleanKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(BOOLEAN_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Boolean kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "Boolean kind should be castable to numeric");
    }

    @Test
    void timestampKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(TIMESTAMP_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Timestamp kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "Timestamp kind should be castable to numeric");
    }

    @Test
    void emailKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(EMAIL_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Email kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "Email kind should be castable to numeric");
    }

    @Test
    void ipAddressKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(IP_ADDRESS_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "IP address kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "IP address kind should be castable to numeric");
    }

    @Test
    void macAddressKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(MAC_ADDRESS_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "MAC address kind should be castable to string");
    }

    @Test
    void phoneNumberKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(PHONE_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Phone number kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "Phone number kind should be castable to numeric");
    }

    @Test
    void creditCardKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(CREDIT_CARD_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Credit card kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "Credit card kind should be castable to numeric");
    }

    @Test
    void valueKind_getsTypeCastFunctions() {
      List<String> functionIds = getFunctionIds(VALUE_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Root value kind should be castable to string");
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_numeric"),
          "Root value kind should be castable to numeric");
    }

    @Test
    void numericKind_getsCorrectJexlTemplatesForCasts() {
      List<TransformationFunctionsByKind> result =
          provider.getFunctionsByKinds(List.of(NUMERIC_KIND));
      assertFalse(result.isEmpty());

      TransformationFunction toStrCast =
          result.get(0).getFunctionsList().stream()
              .filter(f -> f.getId().equals("type_cast_to_system_event_kind_string"))
              .findFirst()
              .orElseThrow();
      assertEquals(
          "traceable:toStr(${input})",
          toStrCast.getJexlTemplate(),
          "Casting numeric to string should use toStr");

      TransformationFunction toIntCast =
          result.get(0).getFunctionsList().stream()
              .filter(f -> f.getId().equals("type_cast_to_system_event_kind_integer"))
              .findFirst()
              .orElseThrow();
      assertEquals(
          "traceable:toNum(${input})",
          toIntCast.getJexlTemplate(),
          "Casting numeric to integer should use toNum");
    }

    @Test
    void numericKind_alsoGetsInheritedValueFunctions() {
      List<String> functionIds = getFunctionIds(NUMERIC_KIND);
      assertTrue(
          functionIds.contains("type_cast_to_system_event_kind_string"),
          "Numeric kind should inherit cast-to-string from value kind");
    }

    @Test
    void numericKind_doesNotGetStringSpecificFunctions() {
      List<String> functionIds = getFunctionIds(NUMERIC_KIND);
      assertFalse(
          functionIds.contains("system_defined_function_split"),
          "Numeric kind should NOT get split (string-only function)");
      assertFalse(
          functionIds.contains("system_defined_function_trim"),
          "Numeric kind should NOT get trim (string-only function)");
      assertFalse(
          functionIds.contains("system_defined_function_base64_encode"),
          "Numeric kind should NOT get base64Encode (string-only function)");
    }

    @Test
    void booleanKind_doesNotGetStringSpecificFunctions() {
      List<String> functionIds = getFunctionIds(BOOLEAN_KIND);
      assertFalse(
          functionIds.contains("system_defined_function_replace"),
          "Boolean kind should NOT get replace (string-only function)");
    }

    @Test
    void stringKind_stillGetsAllExpectedFunctions() {
      List<String> functionIds = getFunctionIds(STRING_KIND);
      assertTrue(functionIds.contains("system_defined_function_split"));
      assertTrue(functionIds.contains("system_defined_function_trim"));
      assertTrue(functionIds.contains("system_defined_function_to_lower_case"));
      assertTrue(functionIds.contains("system_defined_function_to_upper_case"));
      assertTrue(functionIds.contains("system_defined_function_base64_encode"));
      assertTrue(functionIds.contains("system_defined_function_base64_decode"));
      assertTrue(functionIds.contains("type_cast_to_system_event_kind_email"));
      assertTrue(functionIds.contains("type_cast_to_system_event_kind_integer"));
    }

    @Test
    void getAllFunctions_typeCastInputKind_isValueForAll() {
      List<TransformationFunctionsByKind> all = provider.getAllFunctions();
      List<TransformationFunction> typeCasts =
          all.stream()
              .flatMap(m -> m.getFunctionsList().stream())
              .filter(f -> f.getId().startsWith("type_cast_to_"))
              .distinct()
              .collect(Collectors.toList());

      assertFalse(typeCasts.isEmpty(), "Should have type cast functions");
      for (TransformationFunction cast : typeCasts) {
        assertEquals(
            1,
            cast.getInputKindsCount(),
            "Type cast " + cast.getId() + " should have exactly one input kind");
        assertEquals(
            "system_event_kind_value",
            cast.getInputKinds(0).getKindId(),
            "Type cast " + cast.getId() + " input kind should be value (accepts any kind)");
      }
    }

    @Test
    void getAllFunctions_typeCastOutputKind_matchesTargetKind() {
      List<TransformationFunctionsByKind> all = provider.getAllFunctions();
      List<TransformationFunction> typeCasts =
          all.stream()
              .flatMap(m -> m.getFunctionsList().stream())
              .filter(f -> f.getId().startsWith("type_cast_to_"))
              .distinct()
              .collect(Collectors.toList());

      for (TransformationFunction cast : typeCasts) {
        String expectedKindId = cast.getId().replace("type_cast_to_", "");
        assertEquals(
            expectedKindId,
            cast.getOutputKind().getKindId(),
            "Type cast output kind should match the target kind id");
      }
    }

    private List<String> getFunctionIds(ComplexDataModelEventKind kind) {
      List<TransformationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(kind));
      assertFalse(result.isEmpty(), "Should return functions for " + kind);
      return result.get(0).getFunctionsList().stream()
          .map(TransformationFunction::getId)
          .collect(Collectors.toList());
    }
  }

  /**
   * Tests that validate the YAML transformation_functions.yaml is parsed correctly. These tests
   * caught real bugs: regexExtract used 'event_kind_id' (not a proto field) which was silently
   * dropped by ignoringUnknownFields(), making the function invisible.
   */
  @Nested
  class YamlDefinedFunctionIntegrity {

    @Test
    void regexExtract_isDiscoverable_forStringKind() {
      List<String> functionIds = getFunctionIds(STRING_KIND);
      assertTrue(
          functionIds.contains("system_defined_function_regex_extract"),
          "regexExtract must be discoverable - was broken by event_kind_id typo in YAML");
    }

    @Test
    void regexExtract_hasCorrectOutputKind() {
      TransformationFunction fn =
          findFunctionFromAllFunctions("system_defined_function_regex_extract");
      assertTrue(fn.getOutputKind().hasKindId(), "regexExtract output_kind must have kind_id set");
      assertEquals(
          "system_event_kind_string",
          fn.getOutputKind().getKindId(),
          "regexExtract output should be string");
    }

    @Test
    void regexExtract_hasCorrectInputKind() {
      TransformationFunction fn =
          findFunctionFromAllFunctions("system_defined_function_regex_extract");
      assertEquals(1, fn.getInputKindsCount());
      assertTrue(
          fn.getInputKinds(0).hasKindId(),
          "regexExtract input_kind must have kind_id set (was broken by event_kind_id typo)");
      assertEquals("system_event_kind_string", fn.getInputKinds(0).getKindId());
    }

    @Test
    void regexExtract_notAvailableForNonStringKind() {
      ComplexDataModelEventKind numericKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
      List<String> functionIds = getFunctionIds(numericKind);
      assertFalse(
          functionIds.contains("system_defined_function_regex_extract"),
          "regexExtract should NOT be available for numeric kind (input_kinds is string)");
    }

    @Test
    void allYamlFunctions_haveValidInputKinds() {
      List<TransformationFunctionsByKind> all = provider.getAllFunctions();
      List<TransformationFunction> yamlFunctions =
          all.stream()
              .flatMap(m -> m.getFunctionsList().stream())
              .filter(f -> !f.getId().startsWith("type_cast_to_"))
              .distinct()
              .collect(Collectors.toList());

      for (TransformationFunction fn : yamlFunctions) {
        assertTrue(
            fn.getInputKindsCount() > 0,
            "Function " + fn.getId() + " should have at least one input kind");
        for (int i = 0; i < fn.getInputKindsCount(); i++) {
          ComplexDataModelEventKind inputKind = fn.getInputKinds(i);
          assertTrue(
              inputKind.hasKindId()
                  || inputKind.hasArrayOf()
                  || inputKind.hasTypeParameterRef()
                  || inputKind.hasStringMapOf(),
              "Function "
                  + fn.getId()
                  + " input_kind["
                  + i
                  + "] has no type set (possible wrong field name in YAML)");
        }
      }
    }

    @Test
    void allYamlFunctions_haveValidOutputKind() {
      List<TransformationFunctionsByKind> all = provider.getAllFunctions();
      List<TransformationFunction> yamlFunctions =
          all.stream()
              .flatMap(m -> m.getFunctionsList().stream())
              .filter(f -> !f.getId().startsWith("type_cast_to_"))
              .distinct()
              .collect(Collectors.toList());

      for (TransformationFunction fn : yamlFunctions) {
        ComplexDataModelEventKind outputKind = fn.getOutputKind();
        assertTrue(
            outputKind.hasKindId()
                || outputKind.hasArrayOf()
                || outputKind.hasTypeParameterRef()
                || outputKind.hasStringMapOf(),
            "Function "
                + fn.getId()
                + " output_kind has no type set (possible wrong field name in YAML)");
      }
    }

    @Test
    void urlEncode_jexlTemplate_includesQuotePlusParam() {
      TransformationFunction fn =
          findFunctionFromAllFunctions("system_defined_function_url_encode");
      assertEquals(
          "traceableTransformUtils:urlEncode(${input}, false)",
          fn.getJexlTemplate(),
          "urlEncode must pass quotePlus=false to preserve + as literal");
    }

    @Test
    void urlDecode_jexlTemplate_includesQuotePlusParam() {
      TransformationFunction fn =
          findFunctionFromAllFunctions("system_defined_function_url_decode");
      assertEquals(
          "traceableTransformUtils:urlDecode(${input}, false)",
          fn.getJexlTemplate(),
          "urlDecode must pass quotePlus=false to preserve + as literal");
    }

    @Test
    void emailFunctions_restrictedToEmailKind() {
      ComplexDataModelEventKind numericKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_numeric").build();
      List<String> functionIds = getFunctionIds(numericKind);
      assertFalse(
          functionIds.contains("system_defined_function_get_email_domain"),
          "Email domain function should NOT be available for numeric kind");
      assertFalse(
          functionIds.contains("system_defined_function_to_detumbled_email"),
          "Detumbled email function should NOT be available for numeric kind");
    }

    @Test
    void stringFunctions_restrictedToStringKind() {
      ComplexDataModelEventKind boolKind =
          ComplexDataModelEventKind.newBuilder().setKindId("system_event_kind_boolean").build();
      List<String> functionIds = getFunctionIds(boolKind);
      assertFalse(
          functionIds.contains("system_defined_function_base64_encode"),
          "base64Encode should NOT be available for boolean kind (string-only)");
      assertFalse(
          functionIds.contains("system_defined_function_url_encode"),
          "urlEncode should NOT be available for boolean kind (string-only)");
    }

    private TransformationFunction findFunctionFromAllFunctions(String id) {
      return provider.getAllFunctions().stream()
          .flatMap(m -> m.getFunctionsList().stream())
          .filter(f -> f.getId().equals(id))
          .findFirst()
          .orElseThrow(
              () -> new AssertionError("Function " + id + " not found in getAllFunctions"));
    }

    private List<String> getFunctionIds(ComplexDataModelEventKind kind) {
      List<TransformationFunctionsByKind> result = provider.getFunctionsByKinds(List.of(kind));
      assertFalse(result.isEmpty(), "Should return functions for " + kind);
      return result.get(0).getFunctionsList().stream()
          .map(TransformationFunction::getId)
          .collect(Collectors.toList());
    }
  }

  private TransformationFunction findFunction(String id) {
    return provider.getFunctionsByKinds(List.of(STRING_KIND)).get(0).getFunctionsList().stream()
        .filter(f -> f.getId().equals(id))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Function " + id + " not found"));
  }
}
