// Oracle Test Runner for Go SDK
//
// Executes YAML test specifications against the Redlite Go SDK
// and reports pass/fail results with detailed error messages.
//
// Usage:
//   go run go_runner.go                    # Run all specs
//   go run go_runner.go spec/strings.yaml  # Run single spec
//   go run go_runner.go -v                 # Verbose output

package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	redlite "github.com/russellromney/redlite/sdks/redlite-go"
)

// Spec represents a test specification file
type Spec struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	Tests   []Test `yaml:"tests"`
}

// Test represents a single test case
type Test struct {
	Name       string      `yaml:"name"`
	Setup      []Operation `yaml:"setup"`
	Operations []Operation `yaml:"operations"`
}

// Operation represents a single command execution
type Operation struct {
	Cmd    string        `yaml:"cmd"`
	Args   []interface{} `yaml:"args"`
	Kwargs map[string]interface{} `yaml:"kwargs"`
	Expect interface{}   `yaml:"expect"`
}

// Runner executes oracle tests
type Runner struct {
	verbose bool
	passed  int
	failed  int
	skipped int
	errors  []TestError
}

// TestError records a test failure
type TestError struct {
	Spec     string
	Test     string
	Cmd      string
	Expected interface{}
	Actual   interface{}
	Error    string
}

// Commands not supported by Go SDK
var unsupportedCommands = map[string]bool{
	// All commands now supported!
}

func NewRunner(verbose bool) *Runner {
	return &Runner{verbose: verbose}
}

func (r *Runner) RunSpecFile(specPath string) bool {
	data, err := os.ReadFile(specPath)
	if err != nil {
		fmt.Printf("Error reading spec file: %v\n", err)
		return false
	}

	var spec Spec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		fmt.Printf("Error parsing spec file: %v\n", err)
		return false
	}

	specName := spec.Name
	if specName == "" {
		specName = filepath.Base(specPath)
	}

	if r.verbose {
		fmt.Printf("\n%s\n", strings.Repeat("=", 60))
		fmt.Printf("Running: %s (%d tests)\n", specName, len(spec.Tests))
		fmt.Println(strings.Repeat("=", 60))
	}

	for _, test := range spec.Tests {
		r.runTest(test, specName)
	}

	return len(r.errors) == 0
}

func (r *Runner) runTest(test Test, specName string) {
	// Check if test uses unsupported commands
	for _, op := range append(test.Setup, test.Operations...) {
		cmd := strings.ToLower(op.Cmd)
		if unsupportedCommands[cmd] {
			r.skipped++
			if r.verbose {
				fmt.Printf("\n  %s... SKIPPED (unsupported: %s)\n", test.Name, op.Cmd)
			}
			return
		}
	}

	if r.verbose {
		fmt.Printf("\n  %s... ", test.Name)
	}

	// Create fresh in-memory database
	db, err := redlite.OpenEmbeddedMemory()
	if err != nil {
		r.failed++
		r.errors = append(r.errors, TestError{
			Spec:  specName,
			Test:  test.Name,
			Error: fmt.Sprintf("Failed to open database: %v", err),
		})
		if r.verbose {
			fmt.Printf("ERROR: %v\n", err)
		}
		return
	}
	defer db.Close()

	// Run setup operations
	for _, op := range test.Setup {
		if _, err := r.executeCmd(db, op); err != nil {
			r.failed++
			r.errors = append(r.errors, TestError{
				Spec:  specName,
				Test:  test.Name,
				Cmd:   op.Cmd,
				Error: fmt.Sprintf("Setup failed: %v", err),
			})
			if r.verbose {
				fmt.Printf("ERROR: setup %s failed: %v\n", op.Cmd, err)
			}
			return
		}
	}

	// Run test operations
	for _, op := range test.Operations {
		actual, err := r.executeCmd(db, op)
		if err != nil {
			r.failed++
			r.errors = append(r.errors, TestError{
				Spec:  specName,
				Test:  test.Name,
				Cmd:   op.Cmd,
				Error: err.Error(),
			})
			if r.verbose {
				fmt.Printf("ERROR: %v\n", err)
			}
			return
		}

		if !r.compare(actual, op.Expect) {
			r.failed++
			r.errors = append(r.errors, TestError{
				Spec:     specName,
				Test:     test.Name,
				Cmd:      op.Cmd,
				Expected: op.Expect,
				Actual:   r.serialize(actual),
			})
			if r.verbose {
				fmt.Println("FAILED")
				fmt.Printf("      Expected: %v\n", op.Expect)
				fmt.Printf("      Actual:   %v\n", r.serialize(actual))
			}
			return
		}
	}

	r.passed++
	if r.verbose {
		fmt.Println("PASSED")
	}
}

func (r *Runner) executeCmd(db *redlite.EmbeddedDb, op Operation) (interface{}, error) {
	cmd := strings.ToLower(op.Cmd)
	args := op.Args

	switch cmd {
	// String commands
	case "get":
		return db.Get(getString(args, 0))
	case "set":
		key := getString(args, 0)
		value := getBytes(args, 1)
		return true, db.Set(key, value, 0)
	case "setex":
		key := getString(args, 0)
		seconds := getInt64(args, 1)
		value := getBytes(args, 2)
		return true, db.SetEx(key, seconds, value)
	case "psetex":
		key := getString(args, 0)
		milliseconds := getInt64(args, 1)
		value := getBytes(args, 2)
		return true, db.PSetEx(key, milliseconds, value)
	case "getdel":
		return db.GetDel(getString(args, 0))
	case "getrange":
		return db.GetRange(getString(args, 0), getInt64(args, 1), getInt64(args, 2))
	case "setrange":
		return db.SetRange(getString(args, 0), getInt64(args, 1), getBytes(args, 2))
	case "incr":
		return db.Incr(getString(args, 0))
	case "decr":
		return db.Decr(getString(args, 0))
	case "incrby":
		return db.IncrBy(getString(args, 0), getInt64(args, 1))
	case "decrby":
		return db.DecrBy(getString(args, 0), getInt64(args, 1))
	case "incrbyfloat":
		result, err := db.IncrByFloat(getString(args, 0), toFloat64(args[1]))
		if err != nil {
			return nil, err
		}
		// Parse the string result to float for comparison
		var f float64
		if _, parseErr := fmt.Sscanf(result, "%f", &f); parseErr == nil {
			return f, nil
		}
		return result, nil
	case "append":
		return db.Append(getString(args, 0), getBytes(args, 1))
	case "strlen":
		return db.StrLen(getString(args, 0))
	case "mget":
		keys := getStringSlice(args, 0)
		return db.MGet(keys...)
	case "mset":
		pairs := make(map[string][]byte)
		// YAML format: args is [[k1, v1], [k2, v2], [k3, v3]]
		// Each element in args is a [key, value] pair
		for _, pairRaw := range args {
			pair := pairRaw.([]interface{})
			if len(pair) >= 2 {
				key := fmt.Sprintf("%v", pair[0])
				value := toBytes(pair[1])
				pairs[key] = value
			}
		}
		return true, db.MSet(pairs)

	// Key commands
	case "del":
		keys := getStringSlice(args, 0)
		return db.Del(keys...)
	case "exists":
		keys := getStringSlice(args, 0)
		return db.Exists(keys...)
	case "type":
		return db.Type(getString(args, 0))
	case "ttl":
		return db.TTL(getString(args, 0))
	case "pttl":
		return db.PTTL(getString(args, 0))
	case "expire":
		return db.Expire(getString(args, 0), getInt64(args, 1))
	case "pexpire":
		return db.PExpire(getString(args, 0), getInt64(args, 1))
	case "persist":
		return db.Persist(getString(args, 0))
	case "rename":
		return true, db.Rename(getString(args, 0), getString(args, 1))
	case "renamenx":
		return db.RenameNX(getString(args, 0), getString(args, 1))
	case "keys":
		return db.Keys(getString(args, 0))
	case "dbsize":
		return db.DBSize()
	case "flushdb":
		return true, db.FlushDB()

	// Hash commands
	case "hset":
		key := getString(args, 0)
		field := getString(args, 1)
		value := getBytes(args, 2)
		return db.HSet(key, map[string][]byte{field: value})
	case "hget":
		return db.HGet(getString(args, 0), getString(args, 1))
	case "hgetall":
		result, err := db.HGetAll(getString(args, 0))
		if err != nil {
			return nil, err
		}
		// Convert map to flat array of field-value pairs
		var flat [][]byte
		for field, value := range result {
			flat = append(flat, []byte(field))
			flat = append(flat, value)
		}
		return flat, nil
	case "hmget":
		key := getString(args, 0)
		fields := getStringSlice(args, 1)
		return db.HMGet(key, fields...)
	case "hdel":
		key := getString(args, 0)
		fields := getStringSlice(args, 1)
		return db.HDel(key, fields...)
	case "hexists":
		return db.HExists(getString(args, 0), getString(args, 1))
	case "hlen":
		return db.HLen(getString(args, 0))
	case "hkeys":
		return db.HKeys(getString(args, 0))
	case "hvals":
		return db.HVals(getString(args, 0))
	case "hincrby":
		return db.HIncrBy(getString(args, 0), getString(args, 1), getInt64(args, 2))

	// List commands
	case "lpush":
		key := getString(args, 0)
		values := getBytesSlice(args, 1)
		return db.LPush(key, values...)
	case "rpush":
		key := getString(args, 0)
		values := getBytesSlice(args, 1)
		return db.RPush(key, values...)
	case "lpop":
		key := getString(args, 0)
		count := 1
		if len(args) > 1 {
			count = int(getInt64(args, 1))
		}
		result, err := db.LPop(key, count)
		if err != nil {
			return nil, err
		}
		// Normalize to Redis behavior
		if len(args) <= 1 { // no count specified
			if len(result) == 0 {
				return nil, nil
			}
			return result[0], nil
		}
		return result, nil
	case "rpop":
		key := getString(args, 0)
		count := 1
		if len(args) > 1 {
			count = int(getInt64(args, 1))
		}
		result, err := db.RPop(key, count)
		if err != nil {
			return nil, err
		}
		// Normalize to Redis behavior
		if len(args) <= 1 { // no count specified
			if len(result) == 0 {
				return nil, nil
			}
			return result[0], nil
		}
		return result, nil
	case "llen":
		return db.LLen(getString(args, 0))
	case "lrange":
		return db.LRange(getString(args, 0), getInt64(args, 1), getInt64(args, 2))
	case "lindex":
		return db.LIndex(getString(args, 0), getInt64(args, 1))

	// Set commands
	case "sadd":
		key := getString(args, 0)
		members := getBytesSlice(args, 1)
		return db.SAdd(key, members...)
	case "srem":
		key := getString(args, 0)
		members := getBytesSlice(args, 1)
		return db.SRem(key, members...)
	case "smembers":
		return db.SMembers(getString(args, 0))
	case "sismember":
		return db.SIsMember(getString(args, 0), getBytes(args, 1))
	case "scard":
		return db.SCard(getString(args, 0))

	// Sorted set commands
	case "zadd":
		key := getString(args, 0)
		membersRaw := args[1].([]interface{})
		var members []redlite.ZMemberScore
		for _, m := range membersRaw {
			pair := m.([]interface{})
			score := toFloat64(pair[0])
			member := toBytes(pair[1])
			members = append(members, redlite.ZMemberScore{Score: score, Member: member})
		}
		return db.ZAdd(key, members...)
	case "zrem":
		key := getString(args, 0)
		members := getBytesSlice(args, 1)
		return db.ZRem(key, members...)
	case "zrange":
		key := getString(args, 0)
		start := getInt64(args, 1)
		stop := getInt64(args, 2)
		withScores := false
		if len(args) > 3 {
			withScores = getString(args, 3) == "WITHSCORES"
		}
		return db.ZRange(key, start, stop, withScores)
	case "zrevrange":
		key := getString(args, 0)
		start := getInt64(args, 1)
		stop := getInt64(args, 2)
		withScores := false
		if len(args) > 3 {
			withScores = getString(args, 3) == "WITHSCORES"
		}
		return db.ZRevRange(key, start, stop, withScores)
	case "zscore":
		score, exists, err := db.ZScore(getString(args, 0), getBytes(args, 1))
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
		return score, nil
	case "zcard":
		return db.ZCard(getString(args, 0))
	case "zcount":
		return db.ZCount(getString(args, 0), toFloat64(args[1]), toFloat64(args[2]))
	case "zincrby":
		return db.ZIncrBy(getString(args, 0), toFloat64(args[1]), getBytes(args, 2))

	default:
		return nil, fmt.Errorf("unknown command: %s", cmd)
	}
}

// Helper functions for argument extraction
func getString(args []interface{}, idx int) string {
	if idx >= len(args) {
		return ""
	}
	switch v := args[idx].(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func getBytes(args []interface{}, idx int) []byte {
	if idx >= len(args) {
		return nil
	}
	return toBytes(args[idx])
}

func toBytes(v interface{}) []byte {
	switch val := v.(type) {
	case string:
		return []byte(val)
	case []byte:
		return val
	case map[string]interface{}:
		if b, ok := val["bytes"]; ok {
			if arr, ok := b.([]interface{}); ok {
				result := make([]byte, len(arr))
				for i, x := range arr {
					result[i] = byte(toInt64(x))
				}
				return result
			}
		}
		return nil
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

func getInt64(args []interface{}, idx int) int64 {
	if idx >= len(args) {
		return 0
	}
	return toInt64(args[idx])
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}

func getStringSlice(args []interface{}, idx int) []string {
	if idx >= len(args) {
		return nil
	}
	switch v := args[idx].(type) {
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			result[i] = fmt.Sprintf("%v", item)
		}
		return result
	case []string:
		return v
	default:
		return []string{fmt.Sprintf("%v", v)}
	}
}

func getBytesSlice(args []interface{}, idx int) [][]byte {
	if idx >= len(args) {
		return nil
	}
	switch v := args[idx].(type) {
	case []interface{}:
		result := make([][]byte, len(v))
		for i, item := range v {
			result[i] = toBytes(item)
		}
		return result
	default:
		return [][]byte{toBytes(v)}
	}
}

func (r *Runner) compare(actual, expected interface{}) bool {
	if expected == nil {
		// nil expected should match nil, empty byte slice, or empty string
		if actual == nil {
			return true
		}
		if b, ok := actual.([]byte); ok {
			return len(b) == 0
		}
		return false
	}

	switch exp := expected.(type) {
	case map[string]interface{}:
		return r.compareSpecial(actual, exp)
	case bool:
		switch a := actual.(type) {
		case bool:
			return a == exp
		default:
			return false
		}
	case int:
		return r.compareInt(actual, int64(exp))
	case int64:
		return r.compareInt(actual, exp)
	case float64:
		if math.Floor(exp) == exp {
			return r.compareInt(actual, int64(exp))
		}
		return r.compareFloat(actual, exp)
	case string:
		return r.compareString(actual, exp)
	case []interface{}:
		return r.compareList(actual, exp)
	}

	return false
}

func (r *Runner) compareInt(actual interface{}, expected int64) bool {
	switch a := actual.(type) {
	case int64:
		return a == expected
	case int:
		return int64(a) == expected
	case float64:
		return int64(a) == expected
	}
	return false
}

func (r *Runner) compareFloat(actual interface{}, expected float64) bool {
	switch a := actual.(type) {
	case float64:
		return math.Abs(a-expected) < 0.001
	}
	return false
}

func (r *Runner) compareString(actual interface{}, expected string) bool {
	switch a := actual.(type) {
	case []byte:
		return string(a) == expected
	case string:
		return a == expected
	}
	return false
}

func (r *Runner) compareList(actual interface{}, expected []interface{}) bool {
	switch a := actual.(type) {
	case [][]byte:
		if len(a) != len(expected) {
			return false
		}
		for i, item := range a {
			if !r.compare(item, expected[i]) {
				return false
			}
		}
		return true
	case []string:
		if len(a) != len(expected) {
			return false
		}
		for i, item := range a {
			if !r.compare(item, expected[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func (r *Runner) compareSpecial(actual interface{}, expected map[string]interface{}) bool {
	if b, ok := expected["bytes"]; ok {
		arr := b.([]interface{})
		expBytes := make([]byte, len(arr))
		for i, x := range arr {
			expBytes[i] = byte(toInt64(x))
		}
		if actBytes, ok := actual.([]byte); ok {
			return bytes.Equal(actBytes, expBytes)
		}
		return false
	}

	if dict, ok := expected["dict"]; ok {
		expDict := dict.(map[string]interface{})

		// Convert actual (flat array or map) to map
		var actMap map[string]string
		switch a := actual.(type) {
		case [][]byte:
			// Convert flat array of field-value pairs to map
			actMap = make(map[string]string)
			for i := 0; i < len(a); i += 2 {
				if i+1 < len(a) {
					field := string(a[i])
					value := string(a[i+1])
					actMap[field] = value
				}
			}
		case map[string][]byte:
			actMap = make(map[string]string)
			for k, v := range a {
				actMap[k] = string(v)
			}
		default:
			return false
		}

		// Compare maps
		if len(actMap) != len(expDict) {
			return false
		}
		for k, v := range expDict {
			if actMap[k] != fmt.Sprintf("%v", v) {
				return false
			}
		}
		return true
	}

	if set, ok := expected["set"]; ok {
		expSet := set.([]interface{})
		expStrings := make([]string, len(expSet))
		for i, x := range expSet {
			expStrings[i] = fmt.Sprintf("%v", x)
		}
		sort.Strings(expStrings)

		var actStrings []string
		switch a := actual.(type) {
		case [][]byte:
			actStrings = make([]string, len(a))
			for i, x := range a {
				actStrings[i] = string(x)
			}
		case []string:
			actStrings = a
		default:
			return false
		}
		sort.Strings(actStrings)

		if len(actStrings) != len(expStrings) {
			return false
		}
		for i := range actStrings {
			if actStrings[i] != expStrings[i] {
				return false
			}
		}
		return true
	}

	if rng, ok := expected["range"]; ok {
		bounds := rng.([]interface{})
		low := toInt64(bounds[0])
		high := toInt64(bounds[1])
		val := toInt64(actual)
		return val >= low && val <= high
	}

	if approx, ok := expected["approx"]; ok {
		target := toFloat64(approx)
		tol := 0.001
		if t, ok := expected["tol"]; ok {
			tol = toFloat64(t)
		}
		val := toFloat64(actual)
		return math.Abs(val-target) <= tol
	}

	return false
}

func (r *Runner) serialize(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case [][]byte:
		result := make([]string, len(val))
		for i, b := range val {
			result[i] = string(b)
		}
		return result
	}
	return v
}

func (r *Runner) Summary() string {
	total := r.passed + r.failed
	if r.skipped > 0 {
		return fmt.Sprintf("%d/%d passed, %d failed, %d skipped", r.passed, total, r.failed, r.skipped)
	}
	return fmt.Sprintf("%d/%d passed, %d failed", r.passed, total, r.failed)
}

func main() {
	args := os.Args[1:]
	verbose := false
	var specArgs []string

	for _, arg := range args {
		if arg == "-v" || arg == "--verbose" {
			verbose = true
		} else {
			specArgs = append(specArgs, arg)
		}
	}

	// Find spec directory
	specDir := "spec"
	if _, err := os.Stat(specDir); os.IsNotExist(err) {
		// Try relative path from runners/
		specDir = "../spec"
	}

	var specFiles []string
	if len(specArgs) > 0 {
		specFiles = specArgs
	} else {
		entries, err := os.ReadDir(specDir)
		if err != nil {
			fmt.Printf("Error reading spec directory: %v\n", err)
			os.Exit(1)
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".yaml") {
				specFiles = append(specFiles, filepath.Join(specDir, e.Name()))
			}
		}
		sort.Strings(specFiles)
	}

	runner := NewRunner(verbose)

	for _, specFile := range specFiles {
		runner.RunSpecFile(specFile)
	}

	// Print summary
	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	fmt.Printf("Oracle Test Results: %s\n", runner.Summary())
	fmt.Println(strings.Repeat("=", 60))

	if len(runner.errors) > 0 {
		fmt.Println("\nFailures:")
		for _, err := range runner.errors {
			if err.Error != "" {
				fmt.Printf("  - %s / %s: %s\n", err.Spec, err.Test, err.Error)
			} else {
				fmt.Printf("  - %s / %s / %s\n", err.Spec, err.Test, err.Cmd)
				fmt.Printf("      Expected: %v\n", err.Expected)
				fmt.Printf("      Actual:   %v\n", err.Actual)
			}
		}
		os.Exit(1)
	}

	os.Exit(0)
}
