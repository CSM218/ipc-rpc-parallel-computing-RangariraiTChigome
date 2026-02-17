package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic = "CSM218";
    public int version = 1;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    // Backward-compatible aliases some templates expect.
    public String type;
    public String sender;

    public Message() {
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        normalizeAliases();
        validate();
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            DataOutputStream body = new DataOutputStream(bodyBytes);

            writeString(body, magic);
            body.writeInt(version);
            writeString(body, messageType);
            writeString(body, studentId);
            body.writeLong(timestamp);
            body.writeInt(payload == null ? 0 : payload.length);
            if (payload != null) {
                body.write(payload);
            }
            body.flush();

            byte[] bodyBuffer = bodyBytes.toByteArray();
            ByteArrayOutputStream framedBytes = new ByteArrayOutputStream();
            DataOutputStream framed = new DataOutputStream(framedBytes);
            framed.writeInt(bodyBuffer.length);
            framed.write(bodyBuffer);
            framed.flush();
            return framedBytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) {
            throw new IllegalArgumentException("Frame is too short");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            int frameLength = in.readInt();
            if (frameLength < 0 || frameLength > data.length - 4) {
                throw new IllegalArgumentException("Invalid frame length");
            }
            byte[] frame = new byte[frameLength];
            in.readFully(frame);

            DataInputStream frameIn = new DataInputStream(new ByteArrayInputStream(frame));
            Message msg = new Message();
            msg.magic = readString(frameIn);
            msg.version = frameIn.readInt();
            msg.messageType = readString(frameIn);
            msg.studentId = readString(frameIn);
            msg.timestamp = frameIn.readLong();
            int payloadLen = frameIn.readInt();
            if (payloadLen < 0 || payloadLen > frame.length) {
                throw new IllegalArgumentException("Invalid payload length");
            }
            msg.payload = new byte[payloadLen];
            frameIn.readFully(msg.payload);
            msg.type = msg.messageType;
            msg.sender = msg.studentId;
            msg.validate();
            return msg;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }

    public void validate() {
        if (!"CSM218".equals(magic)) {
            throw new IllegalArgumentException("Invalid magic");
        }
        if (version != 1) {
            throw new IllegalArgumentException("Invalid version");
        }
        if (messageType == null || messageType.isBlank()) {
            throw new IllegalArgumentException("Missing messageType");
        }
        if (studentId == null || studentId.isBlank()) {
            throw new IllegalArgumentException("Missing studentId");
        }
        if (payload == null) {
            payload = new byte[0];
        }
    }

    private void normalizeAliases() {
        if ((messageType == null || messageType.isBlank()) && type != null) {
            messageType = type;
        }
        if ((studentId == null || studentId.isBlank()) && sender != null) {
            studentId = sender;
        }
        type = messageType;
        sender = studentId;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IllegalArgumentException("Negative string length");
        }
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
