package dev.aryanyeole.ledgerapi.money;

import org.springframework.boot.jackson.JacksonComponent;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;

/** Amounts go out as JSON strings at scale 2, e.g. "10.00" (D-011, D-047). */
@JacksonComponent
public class MoneySerializer extends ValueSerializer<Money> {

	@Override
	public void serialize(Money value, JsonGenerator generator, SerializationContext context) {
		generator.writeString(value.toString());
	}

}
