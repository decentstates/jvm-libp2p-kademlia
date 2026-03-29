package org.peergos.protocol.dht;

/**
 * Validates incoming PUT_VALUE records before they are stored.
 * Callers who need IPFS/IPNS compatibility should supply an implementation
 * that verifies the record signature and sequence number.
 * Pass {@code Optional.empty()} to accept all records without validation.
 */
public interface RecordValidator {
    boolean validate(byte[] key, byte[] value);
}
