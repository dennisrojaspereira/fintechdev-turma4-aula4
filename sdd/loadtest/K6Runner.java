import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Tiny local server for the classroom load-test page: serves {@code web/index.html} and runs one
 * k6 scenario at a time, streaming its output as Server-Sent Events, then the summary JSON.
 *
 * <pre>
 *   cd sdd/loadtest && java K6Runner.java            # http://localhost:7000
 *   java K6Runner.java 7001                          # another port
 *   K6_MODE=docker java K6Runner.java                # force grafana/k6 in Docker (compose network)
 * </pre>
 * Uses the local {@code k6} binary when present, otherwise {@code docker run grafana/k6} attached
 * to the compose network ({@code sdd_default}). No dependencies beyond the JDK.
 */
public class K6Runner {

    private static final Pattern SCRIPT_NAME = Pattern.compile("^[0-9A-Za-z._-]+\\.js$");
    private static final List<String> PASSTHROUGH_ENV = List.of("RATE", "DURATION");
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private static final Path ROOT = Path.of("").toAbsolutePath();
    private static final Path K6_DIR = ROOT.resolve("k6");
    private static final Path WEB_DIR = ROOT.resolve("web");
    private static final Path RESULTS_DIR = ROOT.resolve("results");

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static String mode; // "local" or "docker"

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
        if (!Files.isDirectory(K6_DIR) || !Files.isDirectory(WEB_DIR)) {
            System.err.println("Run from sdd/loadtest (expected ./k6 and ./web). cwd=" + ROOT);
            System.exit(2);
        }
        Files.createDirectories(RESULTS_DIR);
        mode = detectMode();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/", K6Runner::serveIndex);
        server.createContext("/scenarios", K6Runner::listScenarios);
        server.createContext("/run", K6Runner::run);
        server.createContext("/latest", K6Runner::latest);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("k6 runner (" + mode + ") → http://localhost:" + port + "   scripts: " + K6_DIR);
    }

    // ---------------------------------------------------------------- handlers

    private static void serveIndex(HttpExchange ex) throws IOException {
        if (!"/".equals(ex.getRequestURI().getPath())) {
            respond(ex, 404, "text/plain", "not found");
            return;
        }
        respond(ex, 200, "text/html; charset=utf-8", Files.readString(WEB_DIR.resolve("index.html")));
    }

    private static void listScenarios(HttpExchange ex) throws IOException {
        List<String> names = new ArrayList<>();
        try (Stream<Path> files = Files.list(K6_DIR)) {
            files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".js") && !n.equals("lib.js"))
                    .sorted()
                    .forEach(names::add);
        }
        StringBuilder json = new StringBuilder("{\"mode\":\"").append(mode).append("\",\"scripts\":[");
        for (int i = 0; i < names.size(); i++) {
            json.append(i > 0 ? "," : "").append('"').append(names.get(i)).append('"');
        }
        respond(ex, 200, "application/json", json.append("]}").toString());
    }

    /** The most recent summary JSON of a script, so the page can show results before a rerun. */
    private static void latest(HttpExchange ex) throws IOException {
        String script = query(ex).getOrDefault("script", "");
        if (!SCRIPT_NAME.matcher(script).matches()) {
            respond(ex, 400, "text/plain", "unknown script");
            return;
        }
        String prefix = script.replace(".js", "") + "-";
        Path newest;
        try (Stream<Path> files = Files.list(RESULTS_DIR)) {
            newest = files.filter(p -> p.getFileName().toString().startsWith(prefix)
                            && p.getFileName().toString().endsWith(".json"))
                    .max((a, b) -> a.getFileName().toString().compareTo(b.getFileName().toString()))
                    .orElse(null);
        }
        if (newest == null) {
            respond(ex, 404, "text/plain", "no result yet");
            return;
        }
        String body = "{\"file\":" + jsonString(newest.getFileName().toString()) + ",\"summary\":"
                + Files.readString(newest, StandardCharsets.UTF_8) + "}";
        respond(ex, 200, "application/json", body);
    }

    private static void run(HttpExchange ex) throws IOException {
        Map<String, String> query = query(ex);
        String script = query.getOrDefault("script", "");
        if (!SCRIPT_NAME.matcher(script).matches() || !Files.isRegularFile(K6_DIR.resolve(script))) {
            respond(ex, 400, "text/plain", "unknown script");
            return;
        }
        if (!RUNNING.compareAndSet(false, true)) {
            respond(ex, 409, "text/plain", "a scenario is already running");
            return;
        }
        try {
            stream(ex, script, query);
        } finally {
            RUNNING.set(false);
        }
    }

    // ---------------------------------------------------------------- k6 execution

    private static void stream(HttpExchange ex, String script, Map<String, String> query) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.getResponseHeaders().add("X-Accel-Buffering", "no");
        ex.sendResponseHeaders(200, 0);
        OutputStream out = ex.getResponseBody();

        String runId = LocalDateTime.now().format(TS);
        String summaryName = script.replace(".js", "") + "-" + runId + ".json";
        Path summaryFile = RESULTS_DIR.resolve(summaryName);

        List<String> cmd = command(script, summaryName, runId, query);
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        if ("local".equals(mode)) {
            pb.environment().put("BASE_URL", "http://localhost:8090");
            pb.environment().put("PSP_ADMIN", "http://localhost:8082/__admin");
            pb.environment().put("PIX_ADMIN", "http://localhost:8083/__admin");
            pb.environment().put("RUN_ID", runId);
            for (String key : PASSTHROUGH_ENV) {
                if (query.containsKey(key)) {
                    pb.environment().put(key, query.get(key));
                }
            }
        }

        event(out, "start", "{\"mode\":\"" + mode + "\",\"runId\":\"" + runId + "\",\"command\":"
                + jsonString(String.join(" ", cmd)) + "}");

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            event(out, "log", jsonString("could not start k6: " + e.getMessage()));
            event(out, "done", "{\"exitCode\":-1}");
            out.close();
            return;
        }

        int exit;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder line = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                if (c == '\n' || c == '\r') {
                    if (!line.isEmpty()) {
                        event(out, "log", jsonString(line.toString()));
                        line.setLength(0);
                    }
                } else {
                    line.append((char) c);
                }
            }
            if (!line.isEmpty()) {
                event(out, "log", jsonString(line.toString()));
            }
            exit = process.waitFor();
        } catch (IOException | InterruptedException e) {
            // Browser went away (or we were interrupted): never leave a k6 running.
            process.destroyForcibly();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        if (Files.isRegularFile(summaryFile)) {
            event(out, "summary", Files.readString(summaryFile, StandardCharsets.UTF_8));
        }
        event(out, "done", "{\"exitCode\":" + exit + ",\"summaryFile\":" + jsonString(summaryFile.toString()) + "}");
        out.close();
    }

    private static List<String> command(String script, String summaryName, String runId, Map<String, String> query) {
        List<String> cmd = new ArrayList<>();
        if ("local".equals(mode)) {
            cmd.add("k6");
            cmd.add("run");
            cmd.add("--no-color");
            cmd.add("--summary-export");
            cmd.add(RESULTS_DIR.resolve(summaryName).toString());
            cmd.add(K6_DIR.resolve(script).toString());
            return cmd;
        }
        cmd.addAll(List.of("docker", "run", "--rm", "-i",
                "--network", System.getenv().getOrDefault("K6_NETWORK", "sdd_default"),
                "-v", K6_DIR + ":/scripts:ro",
                "-v", RESULTS_DIR + ":/results",
                "-e", "BASE_URL=http://payments-api:8080",
                "-e", "PSP_ADMIN=http://mock-psp:8080/__admin",
                "-e", "PIX_ADMIN=http://mock-pix:8080/__admin",
                "-e", "RUN_ID=" + runId));
        for (String key : PASSTHROUGH_ENV) {
            if (query.containsKey(key)) {
                cmd.addAll(List.of("-e", key + "=" + query.get(key)));
            }
        }
        cmd.addAll(List.of(System.getenv().getOrDefault("K6_IMAGE", "grafana/k6:latest"),
                "run", "--no-color", "--summary-export", "/results/" + summaryName, "/scripts/" + script));
        return cmd;
    }

    private static String detectMode() {
        String forced = System.getenv("K6_MODE");
        if (forced != null && !forced.isBlank()) {
            return forced;
        }
        try {
            Process p = new ProcessBuilder("k6", "version").redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            return p.waitFor() == 0 ? "local" : "docker";
        } catch (IOException e) {
            return "docker";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "docker";
        }
    }

    // ---------------------------------------------------------------- helpers

    private static void event(OutputStream out, String name, String jsonData) throws IOException {
        out.write(("event: " + name + "\ndata: " + jsonData.replace("\n", "\ndata: ") + "\n\n")
                .getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static void respond(HttpExchange ex, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", contentType);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> map = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null) {
            return map;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            if (value.matches("[0-9A-Za-z._-]*")) {
                map.put(key, value);
            }
        }
        return map;
    }

    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
