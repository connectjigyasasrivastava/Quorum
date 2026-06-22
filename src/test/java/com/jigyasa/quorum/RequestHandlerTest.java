package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestHandlerTest {

    private RequestHandler newHandler(Path dir) throws IOException {
        PersistentMessageQueue queue = new PersistentMessageQueue(dir);
        return new RequestHandler(queue);
    }

    private String encode(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes());
    }

    @Test
    void createTopicReturnsOk(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        String response = handler.handle("CREATE orders");
        assertEquals("OK created", response);
    }

    @Test
    void produceReturnsOffset(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        handler.handle("CREATE orders");
        String response = handler.handle("PRODUCE orders " + encode("hello"));
        assertEquals("OK 0", response);
    }

    @Test
    void consumeReturnsPayload(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        handler.handle("CREATE orders");
        handler.handle("PRODUCE orders " + encode("hello"));

        String response = handler.handle("CONSUME orders 0");
        assertTrue(response.startsWith("OK "));

        String encoded = response.substring(3);
        String decoded = new String(Base64.getDecoder().decode(encoded));
        assertEquals("hello", decoded);
    }

    @Test
    void consumeEmptyOffsetReturnsEmpty(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        handler.handle("CREATE orders");
        String response = handler.handle("CONSUME orders 0");
        assertEquals("EMPTY", response);
    }

    @Test
    void sizeReturnsCount(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        handler.handle("CREATE orders");
        handler.handle("PRODUCE orders " + encode("a"));
        handler.handle("PRODUCE orders " + encode("b"));

        String response = handler.handle("SIZE orders");
        assertEquals("OK 2", response);
    }

    @Test
    void unknownCommandReturnsError(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        String response = handler.handle("FOObar x");
        assertTrue(response.startsWith("ERROR"));
    }

    @Test
    void emptyRequestReturnsError(@TempDir Path dir) throws IOException {
        RequestHandler handler = newHandler(dir);
        String response = handler.handle("");
        assertTrue(response.startsWith("ERROR"));
    }
}