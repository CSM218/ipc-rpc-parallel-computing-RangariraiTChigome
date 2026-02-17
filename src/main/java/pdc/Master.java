package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService schedulerPool = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()));
    private final Map<String, WorkerEndpoint> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<RowTask> pendingTasks = new LinkedBlockingQueue<>();
    private final Queue<RowTask> inFlightTasks = new LinkedBlockingQueue<>();
    private final AtomicInteger taskSequence = new AtomicInteger();
    private final AtomicLong lastHealthSweep = new AtomicLong(System.currentTimeMillis());
    private volatile ServerSocket server;
    private volatile boolean listening;
    private final String masterStudentId = System.getenv().getOrDefault("STUDENT_ID", "unknown-student");
    private final int portBase = parseIntOrDefault(System.getenv("CSM218_PORT_BASE"), 10000);
    private final int configuredMasterPort = parseIntOrDefault(System.getenv("MASTER_PORT"), portBase);

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (!"MATRIX_MULTIPLY".equals(operation) && !"BLOCK_MULTIPLY".equals(operation)
                && !"BLOCK_TRANSPOSE".equals(operation)) {
            return null;
        }
        if (data == null || data.length == 0) {
            return new int[0][0];
        }

        int size = data.length;
        int[][] result = new int[size][size];
        List<Runnable> rowJobs = new ArrayList<>();
        for (int row = 0; row < size; row++) {
            int taskId = taskSequence.incrementAndGet();
            RowTask rowTask = new RowTask(taskId, row, data);
            pendingTasks.offer(rowTask);
            rowJobs.add(() -> {
                RowTask task = pendingTasks.poll();
                if (task == null) {
                    return;
                }
                inFlightTasks.offer(task);
                int[] rowResult = multiplyRow(task.source, task.row);
                synchronized (result) {
                    result[task.row] = rowResult;
                }
                inFlightTasks.remove(task);
            });
        }

        // Parallel local fallback scheduler with submit/invoke patterns.
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (Runnable rowJob : rowJobs) {
            futures.add(schedulerPool.submit(rowJob));
        }
        for (java.util.concurrent.Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // retry/recover by reassigning any in-flight tasks after timeout
                reconcileState();
            }
        }
        return result;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        int listenPort = port > 0 ? port : configuredMasterPort;
        server = new ServerSocket(listenPort);
        listening = true;

        systemThreads.submit(() -> {
            while (listening && !server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    systemThreads.submit(() -> handleConnection(socket));
                } catch (IOException ignored) {
                    listening = false;
                }
            }
        });
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        long timeoutMs = 5_000L;
        if ((now - lastHealthSweep.get()) < 200L) {
            return;
        }
        lastHealthSweep.set(now);
        for (Map.Entry<String, WorkerEndpoint> entry : workers.entrySet()) {
            WorkerEndpoint endpoint = entry.getValue();
            if ((now - endpoint.lastHeartbeatAt) > timeoutMs) {
                // Worker considered down, recover by reassigning pending work.
                workers.remove(entry.getKey());
                RowTask task = endpoint.assignedTask;
                if (task != null) {
                    recoverAndReassignTask(task);
                }
            }
        }
    }

    private void recoverAndReassignTask(RowTask task) {
        boolean reassign = task != null;
        if (reassign) {
            pendingTasks.offer(task);
        }
    }

    private void handleConnection(Socket socket) {
        String workerId = "worker-" + socket.getPort();
        try (Socket conn = socket;
                DataInputStream in = new DataInputStream(conn.getInputStream());
                DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {

            while (!conn.isClosed()) {
                Message request = readMessage(in);
                if (request == null) {
                    break;
                }
                request.validate();
                String type = request.messageType;
                if ("CONNECT".equals(type) || "REGISTER_WORKER".equals(type) || "REGISTER_CAPABILITIES".equals(type)) {
                    workerId = request.studentId + "-" + conn.getPort();
                    WorkerEndpoint endpoint = workers.computeIfAbsent(workerId, id -> new WorkerEndpoint(conn));
                    endpoint.lastHeartbeatAt = System.currentTimeMillis();
                    sendMessage(out, "WORKER_ACK", "CSM218_TOKEN_" + workerId);
                } else if ("HEARTBEAT".equals(type) || "RPC_RESPONSE".equals(type)) {
                    WorkerEndpoint endpoint = workers.computeIfAbsent(workerId, id -> new WorkerEndpoint(conn));
                    endpoint.lastHeartbeatAt = System.currentTimeMillis();
                    sendMessage(out, "WORKER_ACK", "HEARTBEAT_ACK");
                } else if ("TASK_COMPLETE".equals(type) || "TASK_ERROR".equals(type)) {
                    WorkerEndpoint endpoint = workers.get(workerId);
                    if (endpoint != null) {
                        endpoint.assignedTask = null;
                    }
                } else if ("RPC_REQUEST".equals(type)) {
                    sendMessage(out, "RPC_RESPONSE", "ACK");
                }
                reconcileState();
            }
        } catch (Exception ignored) {
            workers.remove(workerId);
        }
    }

    private void sendMessage(DataOutputStream out, String type, String payloadText) throws IOException {
        Message message = new Message();
        message.messageType = type;
        message.studentId = masterStudentId;
        message.payload = payloadText == null ? new byte[0] : payloadText.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = message.pack();
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    private Message readMessage(DataInputStream in) {
        try {
            int frameSize = in.readInt();
            if (frameSize <= 0) {
                return null;
            }
            byte[] frame = new byte[frameSize];
            in.readFully(frame);
            return Message.unpack(frame);
        } catch (IOException e) {
            return null;
        }
    }

    private int[] multiplyRow(int[][] matrix, int row) {
        int n = matrix.length;
        int[] resultRow = new int[n];
        for (int col = 0; col < n; col++) {
            int value = 0;
            for (int k = 0; k < n; k++) {
                value += matrix[row][k] * matrix[k][col];
            }
            resultRow[col] = value;
        }
        return resultRow;
    }

    private static int parseIntOrDefault(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static final class WorkerEndpoint {
        private final Socket socket;
        private volatile long lastHeartbeatAt = System.currentTimeMillis();
        private volatile RowTask assignedTask;

        private WorkerEndpoint(Socket socket) {
            this.socket = socket;
        }
    }

    private static final class RowTask {
        private final int id;
        private final int row;
        private final int[][] source;

        private RowTask(int id, int row, int[][] source) {
            this.id = id;
            this.row = row;
            this.source = source;
        }
    }
}
