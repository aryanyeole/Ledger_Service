package dev.aryanyeole.ledgerapi.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

// The D-143 pin for the balance cache's scripts (D-142). Each constant must equal, byte for byte, a
// literal written out here and the fenced block between its markers in
// docs/design/cache-invalidation.md §2.1 (<!-- script:invalidate --> ... <!-- /script:invalidate -->,
// <!-- script:populate --> ... <!-- /script:populate -->). The worker's Python pin (T8) reads the
// same markers, so the doc is the single text both services are checked against. Line endings are
// normalized to \n (a Windows checkout has CRLF). Tests run from services/ledger-api.
class BalanceCacheScriptsTest {

	private static final Path DOC = Path.of("..", "..", "docs", "design", "cache-invalidation.md");

	@Test
	void invalidateIsTheContractText() throws IOException {
		String literal = "-- ledger:v1 balance invalidation (D-142). KEYS: ledger:v1:bal:{id}, ledger:v1:bal-gen:{id} per account.\n"
				+ "-- ARGV[1]: a fresh token for this call.\n" + "for i = 1, #KEYS, 2 do\n" + "  redis.call('DEL', KEYS[i])\n"
				+ "  redis.call('SET', KEYS[i + 1], ARGV[1], 'PX', 300000)\n" + "end\n" + "return #KEYS\n";
		assertThat(BalanceCacheScripts.INVALIDATE).isEqualTo(literal);
		assertThat(fromDoc("invalidate")).as("doc §2.1 script:invalidate").isEqualTo(BalanceCacheScripts.INVALIDATE);
	}

	@Test
	void populateIsTheContractText() throws IOException {
		String literal = "-- ledger:v1 balance populate (D-142). KEYS[1]: ledger:v1:bal:{id}; KEYS[2]: ledger:v1:bal-gen:{id}.\n"
				+ "-- ARGV[1]: token seen by the MGET ('' if absent); ARGV[2]: the value; ARGV[3]: the value TTL in ms.\n"
				+ "local token = redis.call('GET', KEYS[2])\n" + "if token == false then token = '' end\n"
				+ "if token ~= ARGV[1] then return 0 end\n" + "redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])\n"
				+ "return 1\n";
		assertThat(BalanceCacheScripts.POPULATE).isEqualTo(literal);
		assertThat(fromDoc("populate")).as("doc §2.1 script:populate").isEqualTo(BalanceCacheScripts.POPULATE);
	}

	@Test
	void theTokenPxConstantIsTheScriptsLiteral() {
		assertThat(BalanceCacheScripts.INVALIDATE).contains("'PX', " + BalanceCacheScripts.TOKEN_PX_MILLIS + ")");
	}

	// The body of the ```lua fence between <!-- script:{name} --> and <!-- /script:{name} -->.
	private static String fromDoc(String name) throws IOException {
		String doc = Files.readString(DOC, StandardCharsets.UTF_8).replace("\r\n", "\n");
		Pattern block = Pattern.compile("<!-- script:" + name + " -->\n```lua\n(.*?)```\n<!-- /script:" + name + " -->",
				Pattern.DOTALL);
		Matcher matcher = block.matcher(doc);
		assertThat(matcher.find()).as("markers for %s in %s", name, DOC.toAbsolutePath().normalize()).isTrue();
		String body = matcher.group(1);
		assertThat(matcher.find()).as("exactly one block for %s", name).isFalse();
		return body;
	}

}
