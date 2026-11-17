package dev.aryanyeole.ledgerapi.idempotency;

// Runs inside the owning request's transaction right after its response is stored, before COMMIT
// (D-141): the one deterministic point to roll back an owner whose response already exists. The
// default bean does nothing. No property can arm it: only a test configuration can replace it (a
// @Primary bean, the RelayFaultHook pattern, D-079). The arguments let a test arm it for one key in
// a shared context; the hook never logs them.
@FunctionalInterface
public interface IdempotencyFaultHook {

	void afterStoreBeforeCommit(String clientId, String key);

}
