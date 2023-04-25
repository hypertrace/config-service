package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction.AddAttributeActionTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction.InstructionTranslatorForJwtAction;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.AbstractLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.CookieLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.FormRequestBodyLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.JsonRequestBodyLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.QueryParamLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.RequestHeaderLocationTranslator;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;

public class JwtExtractionTranslationModule extends AbstractModule {
  @Override
  protected void configure() {
    MapBinder<JwtProcessingInstruction.Action.ActionCase, InstructionTranslatorForJwtAction>
        actionTranslatorMapBinder =
            MapBinder.newMapBinder(
                binder(),
                JwtProcessingInstruction.Action.ActionCase.class,
                InstructionTranslatorForJwtAction.class);
    actionTranslatorMapBinder
        .addBinding(JwtProcessingInstruction.Action.ActionCase.ADD_NEW_ATTRIBUTE)
        .to(AddAttributeActionTranslator.class);

    MapBinder<JwtLocation.LocationCase, AbstractLocationTranslator> locationTranslatorMapBinder =
        MapBinder.newMapBinder(
            binder(), JwtLocation.LocationCase.class, AbstractLocationTranslator.class);
    locationTranslatorMapBinder
        .addBinding(JwtLocation.LocationCase.REQUEST_HEADER)
        .to(RequestHeaderLocationTranslator.class);
    locationTranslatorMapBinder
        .addBinding(JwtLocation.LocationCase.REQUEST_COOKIE)
        .to(CookieLocationTranslator.class);
    locationTranslatorMapBinder
        .addBinding(JwtLocation.LocationCase.QUERY_PARAMETER)
        .to(QueryParamLocationTranslator.class);
    locationTranslatorMapBinder
        .addBinding(JwtLocation.LocationCase.FORM_REQUEST_BODY)
        .to(FormRequestBodyLocationTranslator.class);
    locationTranslatorMapBinder
        .addBinding(JwtLocation.LocationCase.JSON_REQUEST_BODY)
        .to(JsonRequestBodyLocationTranslator.class);
  }
}
