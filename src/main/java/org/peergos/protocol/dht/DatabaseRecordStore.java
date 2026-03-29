package org.peergos.protocol.dht;

import io.ipfs.multibase.binary.Base32;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Optional;

public class DatabaseRecordStore implements RecordStore {

    private final String connectionStringPrefix = "jdbc:h2:";
    private final Connection connection;

    private final String RECORD_TABLE = "records";
    private final int SIZE_OF_KEY = 200;

    /*
     * Constructs a DatabaseRecordStore object
     * @param location - location of the database on disk (See: https://h2database.com/html/cheatSheet.html for options)
     */
    public DatabaseRecordStore(String location) {
        try {
            this.connection = DriverManager.getConnection(connectionStringPrefix + location);
            this.connection.setAutoCommit(true);
            createTable();
        } catch (SQLException sqle) {
            throw new IllegalStateException(sqle);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private void createTable() {
        String createSQL = "CREATE TABLE IF NOT EXISTS " + RECORD_TABLE
                + " (record_key VARCHAR(" + SIZE_OF_KEY + ") PRIMARY KEY NOT NULL, record_value BLOB NOT NULL);";
        try (PreparedStatement stmt = connection.prepareStatement(createSQL)) {
            stmt.execute();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private String encodeKey(byte[] key) {
        String padded = new Base32().encodeAsString(key);
        int padStart = padded.indexOf('=');
        return padStart > 0 ? padded.substring(0, padStart) : padded;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        String selectSQL = "SELECT record_value FROM " + RECORD_TABLE + " WHERE record_key=?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, encodeKey(key));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream input = rs.getBinaryStream("record_value")) {
                        byte[] buffer = new byte[1024];
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        for (int len; (len = input.read(buffer)) != -1; ) {
                            bout.write(buffer, 0, len);
                        }
                        return Optional.of(bout.toByteArray());
                    } catch (IOException readEx) {
                        throw new IllegalStateException(readEx);
                    }
                }
                return Optional.empty();
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        String updateSQL = "MERGE INTO " + RECORD_TABLE + " (record_key, record_value) VALUES (?, ?);";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, encodeKey(key));
            pstmt.setBytes(2, value);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void remove(byte[] key) {
        String deleteSQL = "DELETE FROM " + RECORD_TABLE + " WHERE record_key=?";
        try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
            pstmt.setString(1, encodeKey(key));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
