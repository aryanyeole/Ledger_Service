package dev.aryanyeole.ledgerapi.transactions;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;
import dev.aryanyeole.ledgerapi.money.Money;

// GET /v1/transactions?limit=&cursor= (spec §7, D-053): keyset pagination, newest first, over
// (created_at DESC, id DESC). Global for any valid API key (D-069). limit and cursor are read
// as strings so every malformed value gets its own code, not a generic 400.
// Known limitation (D-053): created_at is the posting transaction's start time, so a
// transaction that commits after a traversal has passed its position is not seen by that
// traversal. Fine for browsing; M7's live stream must not rely on it.
@RestController
class TransactionsController {

	static final int DEFAULT_LIMIT = 50;

	static final int MAX_LIMIT = 200;

	private static final Pattern LIMIT = Pattern.compile("[1-9][0-9]{0,2}");

	private final TransactionListing listing;

	TransactionsController(TransactionListing listing) {
		this.listing = listing;
	}

	record EntryItem(long accountId, Money amount) {
	}

	record TransactionItem(long transactionId, String kind, String reference, OffsetDateTime createdAt,
			List<EntryItem> entries) {
	}

	record TransactionPage(List<TransactionItem> data, String nextCursor) {
	}

	@GetMapping("/v1/transactions")
	TransactionPage list(@RequestParam(name = "limit", required = false) String limit,
			@RequestParam(name = "cursor", required = false) String cursor) {
		int pageSize = parseLimit(limit);
		TransactionCursor.Position from = (cursor != null) ? TransactionCursor.decode(cursor) : null;

		// One extra row only decides whether there is a next page.
		List<TransactionListing.Row> rows = this.listing.page(from, pageSize + 1);
		boolean more = rows.size() > pageSize;
		List<TransactionListing.Row> page = more ? rows.subList(0, pageSize) : rows;

		Map<Long, List<TransactionListing.EntryRow>> entries = this.listing
			.entries(page.stream().map(TransactionListing.Row::id).toList());
		List<TransactionItem> items = page.stream()
			.map((row) -> new TransactionItem(row.id(), row.kind(), row.reference(), row.createdAt(),
					entries.getOrDefault(row.id(), List.of())
						.stream()
						// Through Money: scale 2, and a sub-cent stored amount is a 500 (D-056).
						.map((entry) -> new EntryItem(entry.accountId(), new Money(entry.amount())))
						.toList()))
			.toList();
		TransactionListing.Row last = more ? page.get(page.size() - 1) : null;
		return new TransactionPage(items, (last != null) ? TransactionCursor.encode(last.createdAt(), last.id()) : null);
	}

	private static int parseLimit(String limit) {
		if (limit == null) {
			return DEFAULT_LIMIT;
		}
		if (!LIMIT.matcher(limit).matches() || Integer.parseInt(limit) > MAX_LIMIT) {
			throw new ApiException(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_LIMIT,
					"limit must be an integer from 1 to " + MAX_LIMIT + ".");
		}
		return Integer.parseInt(limit);
	}

}
