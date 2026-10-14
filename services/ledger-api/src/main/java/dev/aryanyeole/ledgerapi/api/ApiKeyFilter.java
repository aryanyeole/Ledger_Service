package dev.aryanyeole.ledgerapi.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;

// X-Api-Key -> client_id (D-049). Fail closed: every path needs a key except the health
// probe. The presented key is hashed and compared against every configured hash with
// MessageDigest.isEqual, without stopping at the first match. The key itself is never
// stored, logged, or echoed. Rejections go through the MVC exception resolvers so
// ProblemHandler renders them like every other error.
@Component
@Order(ApiKeyFilter.ORDER)
@EnableConfigurationProperties(ApiKeyProperties.class)
public class ApiKeyFilter extends OncePerRequestFilter {

	public static final String HEADER = "X-Api-Key";

	public static final String CLIENT_ID_ATTRIBUTE = ApiKeyFilter.class.getName() + ".clientId";

	static final int ORDER = CorrelationIdFilter.ORDER + 1;

	private static final String UNAUTHENTICATED_PATH_PREFIX = "/actuator/health";

	private final List<Map.Entry<byte[], String>> clients;

	private final HandlerExceptionResolver exceptionResolver;

	public ApiKeyFilter(ApiKeyProperties properties,
			@Qualifier("handlerExceptionResolver") HandlerExceptionResolver exceptionResolver) {
		this.clients = properties.apiKeys()
			.entrySet()
			.stream()
			.map((e) -> Map.entry(HexFormat.of().parseHex(e.getKey()), e.getValue()))
			.toList();
		this.exceptionResolver = exceptionResolver;
	}

	/** The authenticated client, for handlers behind this filter. */
	public static String clientId(HttpServletRequest request) {
		return (String) request.getAttribute(CLIENT_ID_ATTRIBUTE);
	}

	@Override
	protected boolean shouldNotFilter(HttpServletRequest request) {
		String path = request.getRequestURI().substring(request.getContextPath().length());
		return path.equals(UNAUTHENTICATED_PATH_PREFIX) || path.startsWith(UNAUTHENTICATED_PATH_PREFIX + "/");
	}

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		String key = request.getHeader(HEADER);
		if (key == null || key.isBlank()) {
			reject(request, response, ProblemCode.MISSING_API_KEY, "The X-Api-Key header is required.");
			return;
		}
		String clientId = match(sha256(key));
		if (clientId == null) {
			reject(request, response, ProblemCode.INVALID_API_KEY, "The X-Api-Key header is not a valid API key.");
			return;
		}
		request.setAttribute(CLIENT_ID_ATTRIBUTE, clientId);
		chain.doFilter(request, response);
	}

	private String match(byte[] presented) {
		String matched = null;
		for (Map.Entry<byte[], String> client : this.clients) {
			if (MessageDigest.isEqual(client.getKey(), presented)) {
				matched = client.getValue();
			}
		}
		return matched;
	}

	private void reject(HttpServletRequest request, HttpServletResponse response, ProblemCode code,
			String detail) {
		HttpHeaders challenge = new HttpHeaders();
		challenge.set(HttpHeaders.WWW_AUTHENTICATE, "ApiKey");
		this.exceptionResolver.resolveException(request, response, null,
				new ApiException(HttpStatus.UNAUTHORIZED, code, detail, challenge));
	}

	private static byte[] sha256(String key) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
		}
		catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("SHA-256 is required on every Java platform", ex);
		}
	}

}
