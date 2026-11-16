package dev.aryanyeole.ledgerapi.cache;

import java.net.URI;
import java.net.URISyntaxException;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// ledger.redis.url (D-137), bound only while a cache switch is on (RedisConfiguration). The URL
// must be redis://host[:port] with port 1-65535, no user info, an empty path or /0, and no query or
// fragment; the worker applies the same rule to REDIS_URL. Database 0 is the only one both services
// use, or the worker's invalidations would delete keys the API never reads. No user info keeps a
// password out of config and logs. Error messages name the rule that failed and never echo the URL,
// so a password typed into it can't reach a log.
@ConfigurationProperties("ledger.redis")
public record LedgerRedisProperties(@DefaultValue("redis://localhost:6379") String url) {

	static final int DEFAULT_PORT = 6379;

	public LedgerRedisProperties {
		parse(url);
	}

	public String host() {
		return parse(this.url).getHost();
	}

	public int port() {
		int port = parse(this.url).getPort();
		return (port == -1) ? DEFAULT_PORT : port;
	}

	private static URI parse(String url) {
		if (url == null || url.isBlank()) {
			throw invalid("must not be empty");
		}
		URI uri;
		try {
			uri = new URI(url.strip());
		}
		catch (URISyntaxException ex) {
			throw invalid("is not a valid URI");
		}
		if (!"redis".equalsIgnoreCase(uri.getScheme())) {
			throw invalid("must use the redis:// scheme");
		}
		if (uri.getRawUserInfo() != null || (uri.getRawAuthority() != null && uri.getRawAuthority().contains("@"))) {
			throw invalid("must not contain user info");
		}
		if (uri.getHost() == null || uri.getHost().isEmpty()) {
			throw invalid("must name a host (redis://host[:port])");
		}
		int port = uri.getPort();
		if (port != -1 && (port < 1 || port > 65535)) {
			throw invalid("port must be 1-65535");
		}
		String path = uri.getRawPath();
		if (path != null && !path.isEmpty() && !path.equals("/0")) {
			throw invalid("must address database 0 (an empty path or /0)");
		}
		if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
			throw invalid("must not have a query or fragment");
		}
		return uri;
	}

	private static IllegalArgumentException invalid(String rule) {
		return new IllegalArgumentException("ledger.redis.url " + rule);
	}

}
