package dev.aryanyeole.ledgerapi.api;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

// Every request carries a correlation id: the caller's X-Correlation-Id when it is a safe
// token, otherwise a fresh UUID. It is in the MDC for the whole request (so every log line
// can carry it) and echoed on the response, including error responses from later filters.
// Runs before ApiKeyFilter so 401s carry it too.
@Component
@Order(CorrelationIdFilter.ORDER)
public class CorrelationIdFilter extends OncePerRequestFilter {

	public static final String HEADER = "X-Correlation-Id";

	public static final String MDC_KEY = "correlation_id";

	static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 1;

	private static final String ATTRIBUTE = CorrelationIdFilter.class.getName() + ".id";

	// Caller-supplied ids end up in logs and headers: no whitespace, control characters or
	// separators, and a bounded length. Anything else is replaced, not rejected.
	private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		String id = correlationId(request);
		request.setAttribute(ATTRIBUTE, id);
		response.setHeader(HEADER, id);
		MDC.put(MDC_KEY, id);
		try {
			chain.doFilter(request, response);
		}
		finally {
			MDC.remove(MDC_KEY);
		}
	}

	// The container's error dispatch runs after the original request has left this filter;
	// it reuses the same id so error-page logging stays correlated.
	@Override
	protected boolean shouldNotFilterErrorDispatch() {
		return false;
	}

	private static String correlationId(HttpServletRequest request) {
		if (request.getAttribute(ATTRIBUTE) instanceof String existing) {
			return existing;
		}
		String supplied = request.getHeader(HEADER);
		if (supplied != null && SAFE_ID.matcher(supplied).matches()) {
			return supplied;
		}
		return UUID.randomUUID().toString();
	}

}
