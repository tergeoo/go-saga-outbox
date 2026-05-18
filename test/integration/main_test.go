//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var sharedCluster *cluster

func TestMain(m *testing.M) {
	silenceTestcontainerLogs()

	code := 1
	func() {
		c, err := startCluster()
		if err != nil {
			log.Printf("startCluster: %v", err)
			return
		}
		sharedCluster = c
		defer c.stop()
		code = m.Run()
	}()
	os.Exit(code)
}

type cluster struct {
	ctx    context.Context
	cancel context.CancelFunc

	redpanda    *redpanda.Container
	kafkaBroker string

	dbContainers map[string]*postgres.PostgresContainer
	dbs          map[string]*sqlx.DB

	orchestratorPort int

	processes  map[string]*serviceProcess
	processEnv map[string]map[string]string
	processMu  sync.Mutex

	repoRoot string

	cleanupOnce sync.Once
}

type serviceProcess struct {
	name string
	cmd  *exec.Cmd
	logs *safeBuffer
	wait chan struct{}
}

// stdout and stderr pipe to the same buffer from two goroutines.
type safeBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) WriteString(str string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.WriteString(str)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startCluster() (*cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &cluster{
		ctx:          ctx,
		cancel:       cancel,
		dbContainers: make(map[string]*postgres.PostgresContainer),
		dbs:          make(map[string]*sqlx.DB),
		processes:    make(map[string]*serviceProcess),
		processEnv:   make(map[string]map[string]string),
	}

	root, err := projectRoot()
	if err != nil {
		c.stop()
		return nil, fmt.Errorf("project root: %w", err)
	}
	c.repoRoot = root

	rp, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v23.2.18",
	)
	if err != nil {
		c.stop()
		return nil, fmt.Errorf("start redpanda: %w", err)
	}
	c.redpanda = rp

	brokers, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		c.stop()
		return nil, fmt.Errorf("kafka brokers: %w", err)
	}
	c.kafkaBroker = brokers

	if err := createTopics(c.kafkaBroker, []string{
		"saga.inventory.commands",
		"saga.inventory.events",
		"saga.payment.commands",
		"saga.payment.events",
		"saga.notification.commands",
		"saga.notification.events",
	}); err != nil {
		c.stop()
		return nil, fmt.Errorf("create topics: %w", err)
	}

	type pgResult struct {
		svc       string
		container *postgres.PostgresContainer
		err       error
	}
	dbNames := []string{"orchestrator", "inventory", "payment", "notification"}
	resCh := make(chan pgResult, len(dbNames))
	for _, name := range dbNames {
		name := name
		go func() {
			pg, err := postgres.Run(ctx,
				"docker.io/postgres:16-alpine",
				postgres.WithDatabase(name),
				postgres.WithUsername("postgres"),
				postgres.WithPassword("postgres"),
				testcontainers.WithWaitStrategy(
					wait.ForLog("database system is ready to accept connections").
						WithOccurrence(2).
						WithStartupTimeout(60*time.Second),
				),
			)
			resCh <- pgResult{svc: name, container: pg, err: err}
		}()
	}
	for range dbNames {
		r := <-resCh
		if r.err != nil {
			c.stop()
			return nil, fmt.Errorf("start postgres %s: %w", r.svc, r.err)
		}
		c.dbContainers[r.svc] = r.container
	}

	for _, name := range dbNames {
		dsn, err := c.dbContainers[name].ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			c.stop()
			return nil, fmt.Errorf("conn string %s: %w", name, err)
		}
		db, err := sqlx.Connect("pgx", dsn)
		if err != nil {
			c.stop()
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		c.dbs[name] = db
	}

	port, err := freePort()
	if err != nil {
		c.stop()
		return nil, fmt.Errorf("free port: %w", err)
	}
	c.orchestratorPort = port

	for _, svc := range []string{"inventory", "payment", "notification", "orchestrator"} {
		if err := c.startService(svc); err != nil {
			c.stop()
			return nil, fmt.Errorf("start %s: %w", svc, err)
		}
	}

	// 6. Wait for orchestrator HTTP to accept connections
	if err := waitForHTTP(fmt.Sprintf("localhost:%d", c.orchestratorPort), 60*time.Second); err != nil {
		c.dumpLogs()
		c.stop()
		return nil, fmt.Errorf("orchestrator HTTP not ready: %w", err)
	}

	// 7. Give worker services a beat to finish migrations and join consumer groups.
	// Migrations are quick; kafka rebalance can take a couple of seconds.
	if err := c.waitForServicesReady(20 * time.Second); err != nil {
		c.dumpLogs()
		c.stop()
		return nil, fmt.Errorf("services not ready: %w", err)
	}

	// 8. Seed inventory with free seats for the smoke event_id
	if err := seedInventory(c.dbs["inventory"], testEventID, 10); err != nil {
		c.stop()
		return nil, fmt.Errorf("seed inventory: %w", err)
	}

	return c, nil
}

func (c *cluster) startService(svc string) error {
	c.processMu.Lock()
	defer c.processMu.Unlock()

	if p, ok := c.processes[svc]; ok && p != nil {
		return fmt.Errorf("service %s already running", svc)
	}

	// Кэшируем env на первый запуск, чтобы при restart использовать тот же набор
	// (нужно для orchestrator: SERVER_PORT должен сохраниться).
	envMap, ok := c.processEnv[svc]
	if !ok {
		envMap = c.envForService(svc)
		c.processEnv[svc] = envMap
	}
	env := append(os.Environ(), envMapToSlice(envMap)...)

	cmd := exec.CommandContext(c.ctx, "go", "run", "./"+svc+"/cmd")
	cmd.Dir = c.repoRoot
	cmd.Env = env

	logs := &safeBuffer{}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		_ = cmd.Wait()
	}()

	go pipeToBuilder(stdout, logs, svc+"|out")
	go pipeToBuilder(stderr, logs, svc+"|err")

	c.processes[svc] = &serviceProcess{
		name: svc,
		cmd:  cmd,
		logs: logs,
		wait: waitCh,
	}
	return nil
}

func (c *cluster) stopService(svc string) error {
	c.processMu.Lock()
	p := c.processes[svc]
	c.processes[svc] = nil
	delete(c.processes, svc)
	c.processMu.Unlock()

	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.wait:
	case <-time.After(8 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.wait
	}
	return nil
}

func (c *cluster) isServiceRunning(svc string) bool {
	c.processMu.Lock()
	p := c.processes[svc]
	c.processMu.Unlock()
	if p == nil {
		return false
	}
	select {
	case <-p.wait:
		return false
	default:
		return true
	}
}

func (c *cluster) waitForServiceReady(svc string, timeout time.Duration) error {
	db, ok := c.dbs[svc]
	if !ok {
		return fmt.Errorf("no db for service %s", svc)
	}
	deadline := time.Now().Add(timeout)
	for {
		var n int
		err := db.GetContext(c.ctx, &n, `SELECT 1`)
		if err == nil && n == 1 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s db ping not ready in %s", svc, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *cluster) envForService(svc string) map[string]string {
	kafka := c.kafkaBroker
	pgHost, pgPort := pgEndpoint(c.ctx, c.dbContainers[svc])

	metricsPort, _ := freePort()
	base := map[string]string{
		"MIGRATIONS_DIR":               ".",
		"DB_HOST":                      pgHost,
		"DB_PORT":                      strconv.Itoa(pgPort),
		"DB_USER":                      "postgres",
		"DB_PASS":                      "postgres",
		"DB_NAME":                      svc,
		"DB_SCHEMA":                    "public",
		"OUTBOX_RELAY_POLL_INTERVAL":   "500ms",
		"OUTBOX_RELAY_BATCH_SIZE":      "50",
		"OUTBOX_PRODUCER_BROKERS":      kafka,
		"OUTBOX_PRODUCER_AUTH_ENABLED": "false",
		"METRICS_PORT":                 strconv.Itoa(metricsPort),
	}

	switch svc {
	case "orchestrator":
		base["ORCHESTRATOR_NAME"] = "orchestrator"
		base["SERVER_PORT"] = strconv.Itoa(c.orchestratorPort)
		base["SERVER_TIMEOUT"] = "30s"
		base["INVENTORY_CONSUMER_BROKERS"] = kafka
		base["INVENTORY_CONSUMER_TOPIC"] = "saga.inventory.events"
		base["INVENTORY_CONSUMER_GROUP_ID"] = "orchestrator.inventory.events"
		base["INVENTORY_CONSUMER_MAX_WAIT"] = "50"
		base["INVENTORY_CONSUMER_AUTH_ENABLED"] = "false"
		base["PAYMENT_CONSUMER_BROKERS"] = kafka
		base["PAYMENT_CONSUMER_TOPIC"] = "saga.payment.events"
		base["PAYMENT_CONSUMER_GROUP_ID"] = "orchestrator.payment.events"
		base["PAYMENT_CONSUMER_MAX_WAIT"] = "50"
		base["PAYMENT_CONSUMER_AUTH_ENABLED"] = "false"
		base["NOTIFICATION_CONSUMER_BROKERS"] = kafka
		base["NOTIFICATION_CONSUMER_TOPIC"] = "saga.notification.events"
		base["NOTIFICATION_CONSUMER_GROUP_ID"] = "orchestrator.notification.events"
		base["NOTIFICATION_CONSUMER_MAX_WAIT"] = "50"
		base["NOTIFICATION_CONSUMER_AUTH_ENABLED"] = "false"
	case "payment":
		base["PAYMENT_NAME"] = "payment"
		base["COMMANDS_CONSUMER_BROKERS"] = kafka
		base["COMMANDS_CONSUMER_TOPIC"] = "saga.payment.commands"
		base["COMMANDS_CONSUMER_GROUP_ID"] = "payment.commands"
		base["COMMANDS_CONSUMER_MAX_WAIT"] = "50"
		base["COMMANDS_CONSUMER_AUTH_ENABLED"] = "false"
		// Малые таймауты — иначе после restart consumer'а kafka ждёт 30s до rebalance.
		base["COMMANDS_CONSUMER_SESSION_TIMEOUT"] = "6"
		base["COMMANDS_CONSUMER_HEARTBEAT_INTERVAL"] = "1"
		base["COMMANDS_CONSUMER_REBALANCE_TIMEOUT"] = "10"
	case "inventory":
		base["NAME"] = "inventory"
		fp, _ := freePort()
		base["SERVER_PORT"] = strconv.Itoa(fp)
		base["SERVER_TIMEOUT"] = "30s"
		base["COMMANDS_CONSUMER_BROKERS"] = kafka
		base["COMMANDS_CONSUMER_TOPIC"] = "saga.inventory.commands"
		base["COMMANDS_CONSUMER_GROUP_ID"] = "inventory.commands"
		base["COMMANDS_CONSUMER_MAX_WAIT"] = "50"
		base["COMMANDS_CONSUMER_AUTH_ENABLED"] = "false"
		// Малые таймауты — иначе после restart consumer'а kafka ждёт 30s до rebalance.
		base["COMMANDS_CONSUMER_SESSION_TIMEOUT"] = "6"
		base["COMMANDS_CONSUMER_HEARTBEAT_INTERVAL"] = "1"
		base["COMMANDS_CONSUMER_REBALANCE_TIMEOUT"] = "10"
	case "notification":
		base["NAME"] = "notification"
		base["COMMANDS_CONSUMER_BROKERS"] = kafka
		base["COMMANDS_CONSUMER_TOPIC"] = "saga.notification.commands"
		base["COMMANDS_CONSUMER_GROUP_ID"] = "notification.commands"
		base["COMMANDS_CONSUMER_MAX_WAIT"] = "50"
		base["COMMANDS_CONSUMER_AUTH_ENABLED"] = "false"
		// Малые таймауты — иначе после restart consumer'а kafka ждёт 30s до rebalance.
		base["COMMANDS_CONSUMER_SESSION_TIMEOUT"] = "6"
		base["COMMANDS_CONSUMER_HEARTBEAT_INTERVAL"] = "1"
		base["COMMANDS_CONSUMER_REBALANCE_TIMEOUT"] = "10"
	}
	return base
}

func (c *cluster) waitForServicesReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, svc := range []string{"inventory", "payment", "notification"} {
		db := c.dbs[svc]
		for {
			var exists bool
			err := db.GetContext(c.ctx, &exists,
				`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='outbox')`)
			if err == nil && exists {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%s: outbox table not created within %s (err=%v)", svc, timeout, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	time.Sleep(2 * time.Second)
	return nil
}

func (c *cluster) stop() {
	c.cleanupOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}

		c.processMu.Lock()
		procs := make([]*serviceProcess, 0, len(c.processes))
		for _, p := range c.processes {
			procs = append(procs, p)
		}
		c.processMu.Unlock()

		for _, p := range procs {
			if p == nil || p.cmd == nil || p.cmd.Process == nil {
				continue
			}
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		grace := time.After(5 * time.Second)
		for _, p := range procs {
			if p == nil {
				continue
			}
			select {
			case <-p.wait:
			case <-grace:
				if p.cmd != nil && p.cmd.Process != nil {
					_ = p.cmd.Process.Kill()
				}
			}
		}

		for _, db := range c.dbs {
			_ = db.Close()
		}

		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		for _, pg := range c.dbContainers {
			if pg != nil {
				_ = pg.Terminate(termCtx)
			}
		}
		if c.redpanda != nil {
			_ = c.redpanda.Terminate(termCtx)
		}
	})
}

func (c *cluster) dumpLogs() {
	c.processMu.Lock()
	defer c.processMu.Unlock()
	for _, p := range c.processes {
		if p == nil {
			continue
		}
		log.Printf("--- logs for %s ---\n%s\n", p.name, p.logs.String())
	}
}

const testEventID = "11111111-1111-1111-1111-111111111111"
const testUserID = "22222222-2222-2222-2222-222222222222"

func projectRoot() (string, error) {
	_, here, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(here), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("go.mod not found at %s: %w", root, err)
	}
	return root, nil
}

func pgEndpoint(ctx context.Context, pg *postgres.PostgresContainer) (string, int) {
	host, _ := pg.Host(ctx)
	port, _ := pg.MappedPort(ctx, "5432/tcp")
	n, _ := strconv.Atoi(port.Port())
	return host, n
}

func envMapToSlice(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

var streamLogs = os.Getenv("SAGA_TEST_VERBOSE") != ""

func pipeToBuilder(r io.Reader, b *safeBuffer, prefix string) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			_, _ = b.WriteString(prefix + ": ")
			_, _ = b.Write(buf[:n])
			if streamLogs {
				_, _ = fmt.Fprintf(os.Stderr, "[%s] %s", prefix, buf[:n])
			}
		}
		if err != nil {
			return
		}
	}
}

func freePort() (int, error) {
	a, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", a)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForHTTP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s: %w", addr, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func createTopics(broker string, topics []string) error {
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	addr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := kafkago.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", addr, err)
	}
	defer controllerConn.Close()
	configs := make([]kafkago.TopicConfig, 0, len(topics))
	for _, t := range topics {
		configs = append(configs, kafkago.TopicConfig{
			Topic:             t,
			NumPartitions:     3,
			ReplicationFactor: 1,
		})
	}
	return controllerConn.CreateTopics(configs...)
}

func seedInventory(db *sqlx.DB, eventID string, count int) error {
	for i := 0; i < count; i++ {
		_, err := db.Exec(
			`INSERT INTO seat (id, event_id, status) VALUES (gen_random_uuid(), $1, 'free')`,
			eventID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func silenceTestcontainerLogs() {
	tclog.SetDefault(noopLogger{})
}

type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}

type bookingResponse struct {
	SagaID uuid.UUID `json:"saga_id"`
}

type bookingState struct {
	SagaID      uuid.UUID `json:"saga_id"`
	State       string    `json:"state"`
	CurrentStep string    `json:"current_step"`
}

type sagaStep struct {
	StepName  string         `db:"step_name"`
	Direction string         `db:"direction"`
	Status    string         `db:"status"`
	Error     sql.NullString `db:"error"`
}

func (c *cluster) orchestratorURL(path string) string {
	return fmt.Sprintf("http://localhost:%d%s", c.orchestratorPort, path)
}

func (c *cluster) waitForSagaState(t *testing.T, sagaID uuid.UUID, expected string, timeout time.Duration) bookingState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last bookingState
	for {
		st, err := c.getSagaState(sagaID)
		if err == nil {
			last = st
			if st.State == expected {
				return st
			}
		}
		if time.Now().After(deadline) {
			c.dumpLogs()
			t.Fatalf("saga %s never reached state %q within %s (last=%+v, err=%v)",
				sagaID, expected, timeout, last, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *cluster) sagaSteps(t *testing.T, sagaID uuid.UUID) []sagaStep {
	t.Helper()
	db := c.dbs["orchestrator"]
	var steps []sagaStep
	err := db.SelectContext(c.ctx, &steps,
		`SELECT step_name, direction, status, error
		 FROM saga_step
		 WHERE saga_id = $1
		 ORDER BY created_at`,
		sagaID,
	)
	if err != nil {
		t.Fatalf("read saga_step: %v", err)
	}
	return steps
}
