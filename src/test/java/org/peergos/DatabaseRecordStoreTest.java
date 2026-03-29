package org.peergos;

import org.junit.Assert;
import org.junit.Test;
import org.peergos.protocol.dht.DatabaseRecordStore;

import java.util.Optional;

public class DatabaseRecordStoreTest {

    @Test
    public void testRecordStore() {
        try (DatabaseRecordStore store = new DatabaseRecordStore("mem:")) {
            byte[] key = "testkey".getBytes();
            byte[] value = "testvalue".getBytes();

            store.put(key, value);
            // putting a second time should succeed (upsert)
            store.put(key, value);

            Optional<byte[]> result = store.get(key);
            Assert.assertTrue("Record found", result.isPresent());
            Assert.assertArrayEquals("Value matches", value, result.get());

            store.remove(key);
            Optional<byte[]> deleted = store.get(key);
            Assert.assertTrue("Record removed", deleted.isEmpty());

        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
