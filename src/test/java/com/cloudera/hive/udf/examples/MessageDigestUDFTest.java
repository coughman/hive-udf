package com.cloudera.hive.udf.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class MessageDigestUDFTest {
	MessageDigestUDF udf = new MessageDigestUDF();

	
	@Test
	public void test() {
		String hash = udf.evaluate("kaufman");
		assertNotNull(hash);
		assertEquals("ec202a34f44a632332c71e3700466f7b", hash);
		
		hash = udf.evaluate("cloudera");
		assertNotNull(hash);
		assertEquals("06915a50d28717172b154d86a3399019", hash);
	}

}
