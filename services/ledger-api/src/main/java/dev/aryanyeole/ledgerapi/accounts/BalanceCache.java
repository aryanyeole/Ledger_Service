package dev.aryanyeole.ledgerapi.accounts;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.cache.BalanceCacheScripts;
import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.cache.RedisGateway.Reply;

// The balance cache's read side (D-142, D-151, docs/design/cache-invalidation.md §1.2, §2.1, §3.5,
// §4). Injected only into BalanceController: no class that holds WriteTransactions can reach it, so
// no write path ever reads a cached balance. Invalidation is BalanceCacheInvalidator's, on the write
// side. Both calls go through the gateway's optional(), so both honour the bypass.
//  - read: one MGET of the value and the token. A parseable value is a hit; anything else is a miss
//    (unparseable counted separately) or, when Redis is unavailable, a bypass.
//  - populate: after the DB read, the compare-and-set script with the token the MGET saw ('' for
//    absent): written only if no invalidation ran in between (§4.2). The caller populates only after
//    it has built the BalanceResponse, so a sub-cent balance (D-056's 500) is never cached.
// Never logs a key, value or balance.
public class BalanceCache {

	private static final Logger log = LoggerFactory.getLogger(BalanceCache.class);

	static final String VERSION = "1";

	static final int VALUE_SCALE = 4;

	// D-097's format: UTC, exactly six fractional digits, Z.
	static final DateTimeFormatter AS_OF = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'");

	private static final Pattern BALANCE = Pattern.compile("-?[0-9]+\\.[0-9]{4}");

	private static final Pattern AS_OF_SHAPE = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z");

	enum ReadResult {

		HIT, MISS, BYPASS, UNPARSEABLE

	}

	enum PopulateResult {

		WRITTEN, STALE_TOKEN, UNAVAILABLE

	}

	/** A cached value: the scale-4 balance and the filling read's as_of at UTC. */
	record Entry(BigDecimal balance, OffsetDateTime asOf) {
	}

	/** A read's outcome. entry is set only for a hit; token is what the MGET saw ('' for absent). */
	record Lookup(ReadResult result, Entry entry, String token) {
	}

	/** An entry that can't be parsed; the message names the rule only, never the value. */
	static final class Unparseable extends Exception {

		Unparseable(String rule) {
			super(rule);
		}

	}

	private final RedisGateway gateway;

	private final long ttlMillis;

	private final Map<ReadResult, Counter> reads = new EnumMap<>(ReadResult.class);

	private final Map<PopulateResult, Counter> populates = new EnumMap<>(PopulateResult.class);

	public BalanceCache(RedisGateway gateway, LedgerBalanceCacheProperties properties, MeterRegistry meterRegistry) {
		this.gateway = gateway;
		this.ttlMillis = properties.ttl().toMillis();
		for (ReadResult result : ReadResult.values()) {
			this.reads.put(result, Counter.builder("ledger.balance.cache.read")
				.tag("result", result.name().toLowerCase())
				.register(meterRegistry));
		}
		for (PopulateResult result : PopulateResult.values()) {
			this.populates.put(result, Counter.builder("ledger.balance.cache.populate")
				.tag("result", result.name().toLowerCase())
				.register(meterRegistry));
		}
	}

	/** 1|{balance at scale 4}|{as_of in D-097's format} (§2.1). Throws if balance has digits past 4 dp. */
	static String format(BigDecimal balance, OffsetDateTime asOf) {
		return VERSION + "|" + balance.setScale(VALUE_SCALE, RoundingMode.UNNECESSARY).toPlainString() + "|"
				+ AS_OF.format(asOf.withOffsetSameInstant(ZoneOffset.UTC));
	}

	static Entry parse(String value) throws Unparseable {
		String[] fields = value.split("\\|", -1);
		if (fields.length != 3) {
			throw new Unparseable("not three fields");
		}
		if (!VERSION.equals(fields[0])) {
			throw new Unparseable("unknown version");
		}
		if (!BALANCE.matcher(fields[1]).matches()) {
			throw new Unparseable("balance is not a scale-4 decimal");
		}
		if (!AS_OF_SHAPE.matcher(fields[2]).matches()) {
			throw new Unparseable("as_of is not UTC with six fractional digits");
		}
		OffsetDateTime asOf;
		try {
			asOf = LocalDateTime.parse(fields[2], AS_OF).atOffset(ZoneOffset.UTC);
		}
		catch (DateTimeParseException ex) {
			throw new Unparseable("as_of is not a valid timestamp");
		}
		return new Entry(new BigDecimal(fields[1]), asOf);
	}

	/** The read (§3.5); counts every outcome. */
	Lookup read(long accountId) {
		Reply<List<String>> reply = this.gateway.optional("MGET",
				(redis) -> redis.mget(BalanceCacheScripts.valueKey(accountId), BalanceCacheScripts.tokenKey(accountId)));
		Lookup lookup = lookup(reply);
		this.reads.get(lookup.result()).increment();
		return lookup;
	}

	private static Lookup lookup(Reply<List<String>> reply) {
		if (!reply.available()) {
			return new Lookup(ReadResult.BYPASS, null, null);
		}
		String value = reply.value().get(0);
		String token = (reply.value().get(1) == null) ? "" : reply.value().get(1);
		if (value == null) {
			return new Lookup(ReadResult.MISS, null, token);
		}
		try {
			return new Lookup(ReadResult.HIT, parse(value), token);
		}
		catch (Unparseable ex) {
			log.warn("Balance cache entry unparseable ({}); using the database", ex.getMessage());
			return new Lookup(ReadResult.UNPARSEABLE, null, token);
		}
	}

	/** After the DB read and a built response: the compare-and-set populate (§2.1, §4.2). Never throws. */
	void populate(long accountId, String tokenSeen, BigDecimal balance, OffsetDateTime asOf) {
		String value = format(balance, asOf);
		Reply<Long> reply = this.gateway.optional("EVAL",
				(redis) -> redis.eval(BalanceCacheScripts.POPULATE,
						List.of(BalanceCacheScripts.valueKey(accountId), BalanceCacheScripts.tokenKey(accountId)),
						List.of(tokenSeen, value, Long.toString(this.ttlMillis))));
		PopulateResult result = !reply.available() ? PopulateResult.UNAVAILABLE
				: (reply.value() == 1L) ? PopulateResult.WRITTEN : PopulateResult.STALE_TOKEN;
		this.populates.get(result).increment();
	}

}
