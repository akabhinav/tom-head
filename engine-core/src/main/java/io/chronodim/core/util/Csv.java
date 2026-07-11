package io.chronodim.core.util;

import io.chronodim.api.ValidationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Streaming RFC 4180 CSV reader (quotes, embedded commas/newlines) and writer helper. */
public final class Csv {

    private final BufferedReader reader;
    private int pushback = -2;

    public Csv(Reader reader) {
        this.reader = reader instanceof BufferedReader b ? b : new BufferedReader(reader, 1 << 16);
    }

    /** Reads the next record, or null at end of input. Skips blank lines. */
    public List<String> nextRecord() {
        try {
            while (true) {
                int c = read();
                if (c == -1) return null;
                if (c == '\r' || c == '\n') continue;
                unread(c);
                return readRecord();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> readRecord() throws IOException {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean quoted = false;
        boolean fieldStart = true;
        while (true) {
            int c = read();
            if (quoted) {
                if (c == -1) throw new ValidationException("CSV: unterminated quoted field");
                if (c == '"') {
                    int n = read();
                    if (n == '"') {
                        sb.append('"');
                    } else {
                        quoted = false;
                        if (n != -1) unread(n);
                    }
                } else {
                    sb.append((char) c);
                }
                continue;
            }
            if (c == '"' && fieldStart) {
                quoted = true;
                fieldStart = false;
                continue;
            }
            if (c == ',') {
                fields.add(sb.toString());
                sb.setLength(0);
                fieldStart = true;
                continue;
            }
            if (c == '\r') {
                int n = read();
                if (n != '\n' && n != -1) unread(n);
                fields.add(sb.toString());
                return fields;
            }
            if (c == '\n' || c == -1) {
                fields.add(sb.toString());
                return fields;
            }
            sb.append((char) c);
            fieldStart = false;
        }
    }

    private int read() throws IOException {
        if (pushback != -2) {
            int c = pushback;
            pushback = -2;
            return c;
        }
        return reader.read();
    }

    private void unread(int c) {
        pushback = c;
    }

    /** Escapes one field for CSV output. */
    public static String escape(String v) {
        if (v == null) return "";
        if (v.indexOf(',') < 0 && v.indexOf('"') < 0 && v.indexOf('\n') < 0 && v.indexOf('\r') < 0) return v;
        return '"' + v.replace("\"", "\"\"") + '"';
    }
}
