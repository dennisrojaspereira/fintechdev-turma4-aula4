import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Tiny local server for the classroom page: serves {@code web/index.html}, starts/stops the
 * docker compose stack, reports its readiness, and runs one k6 scenario at a time, streaming
 * output as Server-Sent Events followed by the summary JSON.
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
    /** The compose project lives one level up (sdd/). */
    private static final Path COMPOSE_DIR = ROOT.getParent();

    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final AtomicBoolean STACK_BUSY = new AtomicBoolean(false);
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private static String mode; // "local" or "docker"
    /** Prometheus remote-write URL as seen from k6 (host or compose network), or null if absent. */
    private static volatile String prometheusWriteUrl;

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
        if (!Files.isDirectory(K6_DIR) || !Files.isDirectory(WEB_DIR)) {
            System.err.println("Run from sdd/loadtest (expected ./k6 and ./web). cwd=" + ROOT);
            System.exit(2);
        }
        Files.createDirectories(RESULTS_DIR);
        mode = detectMode();
        prometheusWriteUrl = detectPrometheus();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/", K6Runner::serveIndex);
        server.createContext("/scenarios", K6Runner::listScenarios);
        server.createContext("/run", K6Runner::run);
        server.createContext("/latest", K6Runner::latest);
        server.createContext("/stack/status", K6Runner::stackStatus);
        server.createContext("/stack/up", ex -> stackCommand(ex, "up"));
        server.createContext("/stack/down", ex -> stackCommand(ex, "down"));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("k6 runner (" + mode + ") -> http://localhost:" + port + "   scripts: " + K6_DIR
                + "   compose: " + COMPOSE_DIR
                + (prometheusWriteUrl == null ? "   (no Prometheus yet: detected again on every run)"
                                              : "   metrics -> " + prometheusWriteUrl));
    }

    // ---------------------------------------------------------------- page & scenarios

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
        prometheusWriteUrl = detectPrometheus();
        StringBuilder json = new StringBuilder("{\"mode\":\"").append(mode)
                .append("\",\"prometheus\":").append(prometheusWriteUrl != null)
                .append(",\"scripts\":[");
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
            runScenario(ex, script, query);
        } finally {
            RUNNING.set(false);
        }
    }

    // ---------------------------------------------------------------- k6 execution

    private static void runScenario(HttpExchange ex, String script, Map<String, String> query) throws IOException {
        OutputStream out = openEventStream(ex);
        prometheusWriteUrl = detectPrometheus();

        String runId = LocalDateTime.now().format(TS);
        String summaryName = script.replace(".js", "") + "-" + runId + ".json";
        Path summaryFile = RESULTS_DIR.resolve(summaryName);

        List<String> cmd = k6Command(script, summaryName, runId, query);
        Map<String, String> env = new HashMap<>();
        if ("local".equals(mode)) {
            env.put("BASE_URL", "http://localhost:8090");
            env.put("PSP_ADMIN", "http://localhost:8082/__admin");
            env.put("PIX_ADMIN", "http://localhost:8083/__admin");
            env.put("RUN_ID", runId);
            if (prometheusWriteUrl != null) {
                env.put("K6_PROMETHEUS_RW_SERVER_URL", prometheusWriteUrl);
                env.put("K6_PROMETHEUS_RW_TREND_STATS", "p(95),p(99),avg,max");
            }
            for (String key : PASSTHROUGH_ENV) {
                if (query.containsKey(key)) {
                    env.put(key, query.get(key));
                }
            }
        }

        event(out, "start", "{\"mode\":\"" + mode + "\",\"runId\":\"" + runId + "\",\"command\":"
                + jsonString(String.join(" ", cmd)) + "}");

        // The synthetic probe (compose service) would add its own payments to the app counters
        // the scenarios read (accepted/outcomes): freeze it for the duration of the run.
        docker("pause", "sdd-synthetic");
        Integer exit;
        try {
            exit = streamProcess(out, cmd, env, ROOT);
        } finally {
            docker("unpause", "sdd-synthetic");
        }
        if (exit == null) {
            return; // client went away
        }
        if (Files.isRegularFile(summaryFile)) {
            event(out, "summary", Files.readString(summaryFile, StandardCharsets.UTF_8));
        }
        event(out, "done", "{\"exitCode\":" + exit + ",\"summaryFile\":" + jsonString(summaryFile.toString()) + "}");
        out.close();
    }

    private static List<String> k6Command(String script, String summaryName, String runId, Map<String, String> query) {
        List<String> cmd = new ArrayList<>();
        String testId = script.replace(".js", "");
        if ("local".equals(mode)) {
            cmd.add("k6");
            cmd.add("run");
            cmd.add("--no-color");
            cmd.add("--tag");
            cmd.add("testid=" + testId);
            if (prometheusWriteUrl != null) {
                cmd.add("--out");
                cmd.add("experimental-prometheus-rw");
            }
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
        if (prometheusWriteUrl != null) {
            cmd.addAll(List.of("-e", "K6_PROMETHEUS_RW_SERVER_URL=" + prometheusWriteUrl,
                    "-e", "K6_PROMETHEUS_RW_TREND_STATS=p(95),p(99),avg,max"));
        }
        cmd.addAll(List.of(System.getenv().getOrDefault("K6_IMAGE", "grafana/k6:latest"),
                "run", "--no-color", "--tag", "testid=" + testId));
        if (prometheusWriteUrl != null) {
            cmd.addAll(List.of("--out", "experimental-prometheus-rw"));
        }
        cmd.addAll(List.of("--summary-export", "/results/" + summaryName, "/scripts/" + script));
        return cmd;
    }

    // ---------------------------------------------------------------- docker compose stack

    /**
     * {@code docker compose ps} plus HTTP readiness of the pieces the page cares about:
     * <pre>{"services":[{"service":..,"name":..,"state":..,"health":..}], "ready":{...}}</pre>
     */
    private static void stackStatus(HttpExchange ex) throws IOException {
        List<String> lines = captureLines(List.of("docker", "compose", "ps", "-a", "--format", "json"), COMPOSE_DIR);
        StringBuilder services = new StringBuilder("[");
        boolean first = true;
        for (String line : lines) {
            if (!line.startsWith("{") && !line.startsWith("[")) {
                continue;
            }
            // Some compose versions print one JSON object per line, others a JSON array.
            for (String obj : splitJsonObjects(line)) {
                String service = jsonField(obj, "Service");
                if (service == null) {
                    continue;
                }
                String exitCode = jsonField(obj, "ExitCode");
                services.append(first ? "" : ",").append("{\"service\":").append(jsonString(service))
                        .append(",\"name\":").append(jsonString(String.valueOf(jsonField(obj, "Name"))))
                        .append(",\"state\":").append(jsonString(String.valueOf(jsonField(obj, "State"))))
                        .append(",\"health\":").append(jsonString(String.valueOf(jsonField(obj, "Health"))))
                        .append(",\"exitCode\":").append(exitCode == null ? "null" : exitCode)
                        .append('}');
                first = false;
            }
        }
        services.append(']');

        boolean api = httpOk("http://localhost:8090/actuator/health/readiness", "UP");
        boolean grafana = httpOk("http://localhost:3000/api/health", "ok");
        boolean prometheus = httpOk("http://localhost:9090/-/ready", null);
        boolean connector = httpOk("http://localhost:8084/connectors/payments-outbox/status", "\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"");
        boolean loki = httpOk("http://localhost:3100/ready", "ready");
        boolean tempo = httpOk("http://localhost:3200/ready", "ready");
        String body = "{\"busy\":" + STACK_BUSY.get() + ",\"composeDir\":" + jsonString(COMPOSE_DIR.toString())
                + ",\"services\":" + services
                + ",\"ready\":{\"api\":" + api + ",\"grafana\":" + grafana + ",\"prometheus\":" + prometheus
                + ",\"connector\":" + connector + ",\"loki\":" + loki + ",\"tempo\":" + tempo + "}}";
        respond(ex, 200, "application/json", body);
    }

    /** SSE of {@code docker compose up -d [--build]} or {@code docker compose down}. */
    private static void stackCommand(HttpExchange ex, String verb) throws IOException {
        if (!STACK_BUSY.compareAndSet(false, true)) {
            respond(ex, 409, "text/plain", "the stack is already being changed");
            return;
        }
        try {
            List<String> cmd = new ArrayList<>(List.of("docker", "compose"));
            if ("up".equals(verb)) {
                cmd.addAll(List.of("up", "-d"));
                if ("1".equals(query(ex).getOrDefault("build", "0"))) {
                    cmd.add("--build");
                }
            } else {
                cmd.add("down");
            }
            OutputStream out = openEventStream(ex);
            event(out, "start", "{\"command\":" + jsonString(String.join(" ", cmd)) + "}");
            Integer exit = streamProcess(out, cmd, Map.of(), COMPOSE_DIR);
            if (exit == null) {
                return;
            }
            event(out, "done", "{\"exitCode\":" + exit + "}");
            out.close();
        } finally {
            STACK_BUSY.set(false);
        }
    }

    // ---------------------------------------------------------------- process streaming

    private static OutputStream openEventStream(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.getResponseHeaders().add("X-Accel-Buffering", "no");
        ex.sendResponseHeaders(200, 0);
        return ex.getResponseBody();
    }

    /**
     * Runs {@code cmd}, forwarding every output line as a {@code log} event. Returns the exit
     * code, or null if the client disconnected (the process is then killed).
     */
    private static Integer streamProcess(OutputStream out, List<String> cmd, Map<String, String> env, Path dir) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true).directory(dir.toFile());
        pb.environment().putAll(env);
        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            event(out, "log", jsonString("could not start " + cmd.get(0) + ": " + e.getMessage()));
            event(out, "done", "{\"exitCode\":-1}");
            out.close();
            return null;
        }
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
            return process.waitFor();
        } catch (IOException | InterruptedException e) {
            // Browser went away (or we were interrupted): never leave a child process running.
            process.destroyForcibly();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    private static List<String> captureLines(List<String> cmd, Path dir) {
        List<String> lines = new ArrayList<>();
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).directory(dir.toFile()).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    lines.add(line);
                }
            }
            p.waitFor();
        } catch (IOException e) {
            lines.add("error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return lines;
    }

    // ---------------------------------------------------------------- detection helpers

    /** Prometheus of the compose stack (host port 9090); k6 remote-writes to it when it answers. */
    private static String detectPrometheus() {
        String forced = System.getenv("K6_PROMETHEUS_RW_SERVER_URL");
        if (forced != null && !forced.isBlank()) {
            return forced;
        }
        if (!httpOk("http://localhost:9090/-/ready", null)) {
            return null;
        }
        return "local".equals(mode) ? "http://localhost:9090/api/v1/write" : "http://prometheus:9090/api/v1/write";
    }

    private static boolean httpOk(String url, String mustContain) {
        try {
            HttpResponse<String> res = HTTP.send(HttpRequest.newBuilder().uri(URI.create(url))
                    .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
            return res.statusCode() == 200 && (mustContain == null || res.body().contains(mustContain));
        } catch (IOException | IllegalArgumentException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Best effort {@code docker <verb> <container>}; silently ignored when Docker or the container is absent. */
    private static void docker(String verb, String container) {
        try {
            Process p = new ProcessBuilder("docker", verb, container).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            p.waitFor();
        } catch (IOException e) {
            // no docker on this machine: nothing to pause
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    // ---------------------------------------------------------------- tiny JSON helpers

    /** Splits a line that is either one JSON object or a JSON array of flat objects. */
    private static List<String> splitJsonObjects(String line) {
        List<String> objects = new ArrayList<>();
        int depth = 0;
        int start = -1;
        boolean inString = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"' && (i == 0 || line.charAt(i - 1) != '\\')) {
                inString = !inString;
            }
            if (inString) {
                continue;
            }
            if (c == '{') {
                if (depth++ == 0) {
                    start = i;
                }
            } else if (c == '}' && --depth == 0 && start >= 0) {
                objects.add(line.substring(start, i + 1));
                start = -1;
            }
        }
        return objects;
    }

    /** Value of a top-level string/number field of a flat JSON object, or null. */
    private static String jsonField(String obj, String field) {
        Matcher m = Pattern.compile("\"" + field + "\"\\s*:\\s*(\"((?:[^\"\\\\]|\\\\.)*)\"|(-?\\d+))").matcher(obj);
        if (!m.find()) {
            return null;
        }
        return m.group(2) != null ? m.group(2).replace("\\\"", "\"").replace("\\\\", "\\") : m.group(3);
    }

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
