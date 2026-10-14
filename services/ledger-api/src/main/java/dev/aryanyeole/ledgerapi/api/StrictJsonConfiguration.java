package dev.aryanyeole.ledgerapi.api;

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.type.LogicalType;

// Strict JSON scalars (D-065): integer fields accept only JSON integers. Jackson's defaults
// would turn 5.9 into 5 and "5" into 5, silently posting to the wrong account. A rejected
// value surfaces as a databind error, which ProblemHandler maps to 400 invalid_request.
// Amounts are unaffected: MoneyDeserializer accepts only strings (D-047, invalid_amount).
// Unknown properties are rejected by spring.jackson.deserialization.fail-on-unknown-properties.
@Configuration(proxyBeanMethods = false)
public class StrictJsonConfiguration {

	@Bean
	JsonMapperBuilderCustomizer strictIntegerCoercion() {
		return (builder) -> builder.withCoercionConfig(LogicalType.Integer,
				(config) -> config.setCoercion(CoercionInputShape.String, CoercionAction.Fail)
					.setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
					.setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail)
					.setCoercion(CoercionInputShape.EmptyString, CoercionAction.Fail));
	}

}
