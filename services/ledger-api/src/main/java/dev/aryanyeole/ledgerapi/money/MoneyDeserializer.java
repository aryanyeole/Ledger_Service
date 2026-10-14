package dev.aryanyeole.ledgerapi.money;

import java.math.BigDecimal;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jackson.JacksonComponent;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.util.AccessPattern;

// Request amounts (D-047): a JSON string of plain decimal digits, at most 2 dp, > 0 and
// <= ledger.money.max-amount. JSON numbers never reach BigDecimal parsing, so no double is
// ever involved; exponents, signs, leading zeros and blanks fail the pattern. The value is
// normalized to scale 2 and never rounded: more than 2 dp is rejected, not rounded.
// Null and absent amounts are rejected here too, so every amount problem is invalid_amount.
@JacksonComponent
@EnableConfigurationProperties(MoneyProperties.class)
public class MoneyDeserializer extends ValueDeserializer<Money> {

	// Matcher.matches() is a whole-input match, and \d is ASCII-only without
	// UNICODE_CHARACTER_CLASS, so a trailing newline or non-ASCII digits fail.
	private static final Pattern AMOUNT = Pattern.compile("(0|[1-9]\\d*)(\\.\\d{1,2})?");

	private final BigDecimal maxAmount;

	public MoneyDeserializer(MoneyProperties properties) {
		this.maxAmount = properties.maxAmount();
	}

	@Override
	public Money deserialize(JsonParser parser, DeserializationContext context) {
		if (!parser.hasToken(JsonToken.VALUE_STRING)) {
			throw new InvalidAmountException("amount must be a JSON string such as \"10.00\".");
		}
		return parse(parser.getString());
	}

	Money parse(String text) {
		if (!AMOUNT.matcher(text).matches()) {
			throw new InvalidAmountException(
					"amount must be a decimal string with digits only and at most 2 decimal places, such as \"10.00\".");
		}
		BigDecimal value = new BigDecimal(text);
		if (value.signum() <= 0) {
			throw new InvalidAmountException("amount must be greater than 0.");
		}
		if (value.compareTo(this.maxAmount) > 0) {
			throw new InvalidAmountException("amount must not exceed " + new Money(this.maxAmount) + ".");
		}
		return new Money(value);
	}

	@Override
	public Money getNullValue(DeserializationContext context) {
		throw new InvalidAmountException("amount is required.");
	}

	@Override
	public Object getAbsentValue(DeserializationContext context) {
		return getNullValue(context);
	}

	// The null value is an exception, so it must be asked for every time, never cached.
	@Override
	public AccessPattern getNullAccessPattern() {
		return AccessPattern.DYNAMIC;
	}

}
