package dev.aryanyeole.ledgerapi.outbox;

import java.util.List;

// Runs inside the relay's batch transaction after every record of the batch has been acked and
// before the rows are marked published (D-079): the one deterministic duplicate seam, where the
// broker has the records but the database doesn't know it yet. The default bean does nothing. No
// property can arm it: only a test configuration can replace it, e.g. to fail there and show the
// batch is rolled back and published again with byte-identical copies (OutboxRedeliveryTest).
@FunctionalInterface
public interface RelayFaultHook {

	void afterAcksBeforeMark(List<Long> ids);

}
