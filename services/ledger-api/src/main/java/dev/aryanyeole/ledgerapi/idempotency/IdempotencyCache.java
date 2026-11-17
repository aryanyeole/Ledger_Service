package dev.aryanyeole.ledgerapi.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import dev.aryanyeole.ledgerapi.cache.RedisGateway;
import dev.aryanyeole.ledgerapi.cache.RedisGateway.Reply;

// The idempotency fast path's Redis side (D-139, docs/design/cache-invalidation.md §1.1, §2.1).
// Redis may decide only "replay these exact bytes": a hit whose stored fingerprint equals the
// request's. A mismatch, a miss, a bypass and an unparseable entry all go to the DB path, which
// alone decides 422, 409 and whether a key is new. Entries are written only after the transaction
// has ended, from the DB's own response_body::text, and only while the DB says the row has more than
// ttl + margin left; SET NX never overwrites. Both calls go through the gateway's optional(): neither
// is an invalidation, so both honour the bypass. Never logs a key, value, hash or body.
public class IdempotencyCache {

	private static final Logger log = LoggerFactory.getLogger(IdempotencyCache.class);

	static final String VERSION = "1";

	private static final HexFormat HEX = HexFormat.of();

	private static final Pattern HASH = Pattern.compile("[0-9a-f]{64}");

	enum ReadResult {

		HIT, MISS, BYPASS, MISMATCH, UNPARSEABLE

	}

	enum PopulateResult {

		WRITTEN, EXISTS, SKIPPED_SHORT_LIFE, UNAVAILABLE

	}

	/** A read's outcome; status and text are set only for a hit. */
	record Lookup(ReadResult result, int status, String text) {

		static Lookup of(ReadResult result) {
			return new Lookup(result, 0, null);
		}

	}

	record Entry(byte[] requestHash, int status, String text) {
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

	public IdempotencyCache(RedisGateway gateway, Duration ttl, MeterRegistry meterRegistry) {
		this.gateway = gateway;
		this.ttlMillis = ttl.toMillis();
		for (ReadResult result : ReadResult.values()) {
			this.reads.put(result, Counter.builder("ledger.idempotency.cache.read")
				.tag("result", result.name().toLowerCase())
				.register(meterRegistry));
		}
		for (PopulateResult result : PopulateResult.values()) {
			this.populates.put(result, Counter.builder("ledger.idempotency.cache.populate")
				.tag("result", result.name().toLowerCase())
				.register(meterRegistry));
		}
	}

	/** ledger:v1:idem:{len}:{client_id}:{key}, len = client_id's UTF-8 byte length (§2.1). */
	static String key(String clientId, String key) {
		return "ledger:v1:idem:" + clientId.getBytes(StandardCharsets.UTF_8).length + ":" + clientId + ":" + key;
	}

	/** 1|{hex(request_hash)}|{status}|{text} (§2.1). */
	static String format(byte[] requestHash, int status, String text) {
		return VERSION + "|" + HEX.formatHex(requestHash) + "|" + status + "|" + text;
	}

	/** Splits on the first three '|' only: the text may contain '|'. */
	static Entry parse(String value) throws Unparseable {
		String[] fields = value.split("\\|", 4);
		if (fields.length < 4) {
			throw new Unparseable("fewer than four fields");
		}
		if (!VERSION.equals(fields[0])) {
			throw new Unparseable("unknown version");
		}
		if (!HASH.matcher(fields[1]).matches()) {
			throw new Unparseable("request hash is not 64 lowercase hex digits");
		}
		int status;
		try {
			status = Integer.parseInt(fields[2]);
		}
		catch (NumberFormatException ex) {
			throw new Unparseable("status is not an integer");
		}
		if (status < 100 || status > 599) {
			throw new Unparseable("status is not 100-599");
		}
		return new Entry(HEX.parseHex(fields[1]), status, fields[3]);
	}

	/** The fast-path read (§3.1); counts every outcome. */
	Lookup read(String clientId, String key, byte[] requestHash) {
		Reply<String> reply = this.gateway.optional("GET", (redis) -> redis.get(key(clientId, key)));
		Lookup lookup = lookup(reply, requestHash);
		this.reads.get(lookup.result()).increment();
		return lookup;
	}

	private static Lookup lookup(Reply<String> reply, byte[] requestHash) {
		if (!reply.available()) {
			return Lookup.of(ReadResult.BYPASS);
		}
		if (reply.value() == null) {
			return Lookup.of(ReadResult.MISS);
		}
		Entry entry;
		try {
			entry = parse(reply.value());
		}
		catch (Unparseable ex) {
			log.warn("Idempotency cache entry unparseable ({}); using the database", ex.getMessage());
			return Lookup.of(ReadResult.UNPARSEABLE);
		}
		if (!MessageDigest.isEqual(entry.requestHash(), requestHash)) {
			return Lookup.of(ReadResult.MISMATCH);
		}
		return new Lookup(ReadResult.HIT, entry.status(), entry.text());
	}

	/**
	 * After the transaction has ended: SET NX PX ttl if the DB said the row has enough life left,
	 * otherwise count skipped_short_life. Never throws.
	 */
	void populate(String clientId, String key, byte[] requestHash, int status, String text, boolean cacheable) {
		if (!cacheable) {
			this.populates.get(PopulateResult.SKIPPED_SHORT_LIFE).increment();
			return;
		}
		String value = format(requestHash, status, text);
		Reply<Boolean> reply = this.gateway.optional("SET",
				(redis) -> redis.setNxPx(key(clientId, key), value, this.ttlMillis));
		PopulateResult result = !reply.available() ? PopulateResult.UNAVAILABLE
				: Boolean.TRUE.equals(reply.value()) ? PopulateResult.WRITTEN : PopulateResult.EXISTS;
		this.populates.get(result).increment();
	}

}
