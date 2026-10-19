package dev.aryanyeole.ledgerapi.outbox;

import java.util.List;

// Runs inside the relay's batch transaction after every record has been handed to the producer
// and before the relay waits for the acks and marks the rows (D-079). The default bean does
// nothing. No property can arm it: only a test configuration can replace it, e.g. to fail
// after the sends and show that the batch rolls back and is published again (T5).
@FunctionalInterface
public interface RelayFaultHook {

	void afterSendBeforeMark(List<Long> ids);

}
