package pipeline

import (
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t14raptor/go-fast/ast"
	"github.com/t14raptor/go-fast/generator"
	"github.com/t14raptor/go-fast/parser"
	"github.com/t14raptor/go-fast/resolver"
)

// serialResult produces the reference output by running the three stages in
// sequence, exactly as a caller would without the pipeline.
func serialResult(t *testing.T, src string) Result {
	t.Helper()
	prog, err := parser.Parse(src)
	if err != nil {
		return Result{Err: err, Stage: StageParse}
	}
	resolver.Resolve(prog)
	return Result{Source: generator.Generate(prog)}
}

func validSources() []string {
	return []string{
		`const a = 1;`,
		`function f(x) { return x + 1; } f(2);`,
		`let obj = { a: 1, b: [2, 3] }; for (const k in obj) { console.log(k); }`,
		`class A { #x = 1; get x() { return this.#x; } } new A().x;`,
		`const f = (a, b = 2, ...rest) => a + b + rest.length; f(1);`,
		`try { throw new Error("x"); } catch (e) { console.log(e.message); }`,
		`for (let i = 0; i < 3; i++) { if (i % 2) continue; }`,
		`switch (1) { case 1: break; default: break; }`,
	}
}

func TestProcessSuccessMatchesSerial(t *testing.T) {
	sources := validSources()
	results := Process(sources, 3)

	if len(results) != len(sources) {
		t.Fatalf("len(results) = %d; want %d", len(results), len(sources))
	}
	for i, src := range sources {
		want := serialResult(t, src)
		got := results[i]
		if !got.OK() {
			t.Fatalf("results[%d] failed at stage %s: %v", i, got.Stage, got.Err)
		}
		if got.Source != want.Source {
			t.Errorf("results[%d] = %q; want %q", i, got.Source, want.Source)
		}
		if got.Stage != StageNone {
			t.Errorf("results[%d].Stage = %s; want %s", i, got.Stage, StageNone)
		}
	}
}

func TestProcessPreservesInputOrder(t *testing.T) {
	const n = 60
	sources := make([]string, n)
	for i := range sources {
		sources[i] = "var value" + strconv.Itoa(i) + " = " + strconv.Itoa(i) + ";"
	}

	results := Process(sources, 4)
	if len(results) != n {
		t.Fatalf("len(results) = %d; want %d", len(results), n)
	}
	for i := range sources {
		want := "var value" + strconv.Itoa(i) + " = " + strconv.Itoa(i) + ";\n"
		if results[i].Err != nil {
			t.Fatalf("results[%d] failed: %v", i, results[i].Err)
		}
		if results[i].Source != want {
			t.Errorf("results[%d] = %q; want %q", i, results[i].Source, want)
		}
	}
}

func TestProcessParseFailureIsIsolated(t *testing.T) {
	sources := []string{
		`const ok1 = 1;`,
		`const = ;`, // parse failure
		`const ok2 = 2;`,
		`function (`, // parse failure
		`const ok3 = 3;`,
	}
	results := Process(sources, 2)

	for i, src := range sources {
		got := results[i]
		switch i {
		case 1, 3:
			if got.Err == nil {
				t.Fatalf("results[%d] succeeded; want parse failure", i)
			}
			if got.Stage != StageParse {
				t.Errorf("results[%d].Stage = %s; want %s", i, got.Stage, StageParse)
			}
			if got.Source != "" {
				t.Errorf("results[%d].Source = %q; want empty", i, got.Source)
			}
		default:
			want := serialResult(t, src)
			if got.Err != nil {
				t.Fatalf("results[%d] failed: %v", i, got.Err)
			}
			if got.Source != want.Source {
				t.Errorf("results[%d] = %q; want %q", i, got.Source, want.Source)
			}
		}
	}
}

// TestProcessLaterStageFailuresRecordStage checks that a failure after a
// successful earlier stage is attributed to the stage that failed, and that a
// parse failure never reaches bind or generate.
func TestProcessLaterStageFailuresRecordStage(t *testing.T) {
	var mu sync.Mutex
	reached := map[string]map[int]bool{}
	mark := func(stage string, index int) {
		mu.Lock()
		defer mu.Unlock()
		if reached[stage] == nil {
			reached[stage] = map[int]bool{}
		}
		reached[stage][index] = true
	}

	st := stages{
		parse: func(index int, src string) (*ast.Program, error) {
			mark("parse", index)
			return parser.Parse(src)
		},
		bind: func(index int, prog *ast.Program) error {
			mark("bind", index)
			if index == 2 {
				return errors.New("bind failed")
			}
			resolver.Resolve(prog)
			return nil
		},
		gen: func(index int, prog *ast.Program) (string, error) {
			mark("generate", index)
			if index == 4 {
				return "", errors.New("generate failed")
			}
			return generator.Generate(prog), nil
		},
	}

	sources := []string{
		`const a = 1;`, // success
		`const = ;`,    // parse failure at 1
		`const b = 2;`, // bind failure at 2
		`const c = 3;`, // success
		`const d = 4;`, // generate failure at 4
		`const e = 5;`, // success
	}
	results := run(sources, 2, st)

	wantStage := map[int]Stage{
		1: StageParse,
		2: StageBind,
		4: StageGenerate,
	}
	for i := range sources {
		got := results[i]
		if stage, failed := wantStage[i]; failed {
			if got.Err == nil {
				t.Fatalf("results[%d] succeeded; want failure at %s", i, stage)
			}
			if got.Stage != stage {
				t.Errorf("results[%d].Stage = %s; want %s", i, got.Stage, stage)
			}
		} else {
			want := serialResult(t, sources[i])
			if got.Err != nil {
				t.Fatalf("results[%d] failed: %v", i, got.Err)
			}
			if got.Source != want.Source {
				t.Errorf("results[%d] = %q; want %q", i, got.Source, want.Source)
			}
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if reached["bind"][1] || reached["generate"][1] {
		t.Error("parse failure at 1 reached a later stage")
	}
	if reached["generate"][2] {
		t.Error("bind failure at 2 reached generate")
	}
}

// TestProcessBackpressureKeepsInFlightBounded verifies that a batch much
// larger than concurrency does not cause every AST to be in flight at once.
//
// In-flight items are counted when a parse worker picks a source up and
// released when its generate worker finishes. The bound is the number of
// workers in the three stages (3*concurrency) plus the two inter-stage
// channel buffers (2*concurrency).
func TestProcessBackpressureKeepsInFlightBounded(t *testing.T) {
	const concurrency = 3
	const n = 200

	var inFlight, maxInFlight atomic.Int64
	st := defaultStages()
	origParse, origGen := st.parse, st.gen
	st.parse = func(index int, src string) (*ast.Program, error) {
		cur := inFlight.Add(1)
		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}
		return origParse(index, src)
	}
	st.gen = func(index int, prog *ast.Program) (string, error) {
		defer inFlight.Add(-1)
		return origGen(index, prog)
	}

	sources := make([]string, n)
	for i := range sources {
		sources[i] = "var x" + strconv.Itoa(i) + " = " + strconv.Itoa(i) + ";"
	}

	results := run(sources, concurrency, st)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("results[%d] failed: %v", i, r.Err)
		}
	}

	const bound = 5 * concurrency
	if got := maxInFlight.Load(); got > bound {
		t.Errorf("max in-flight = %d; want <= %d", got, bound)
	}
	if got := maxInFlight.Load(); got >= n {
		t.Errorf("max in-flight = %d; want well below batch size %d", got, n)
	}
}

func TestProcessNonPositiveConcurrencyUsesDefault(t *testing.T) {
	sources := validSources()
	for _, concurrency := range []int{0, -1, -100} {
		results := Process(sources, concurrency)
		if len(results) != len(sources) {
			t.Fatalf("concurrency %d: len(results) = %d; want %d", concurrency, len(results), len(sources))
		}
		for i, src := range sources {
			want := serialResult(t, src)
			if results[i].Err != nil {
				t.Fatalf("concurrency %d: results[%d] failed: %v", concurrency, i, results[i].Err)
			}
			if results[i].Source != want.Source {
				t.Errorf("concurrency %d: results[%d] = %q; want %q", concurrency, i, results[i].Source, want.Source)
			}
		}
	}
}

func TestProcessEmptyInput(t *testing.T) {
	base := runtime.NumGoroutine()

	for _, sources := range [][]string{nil, {}} {
		results := Process(sources, 4)
		if len(results) != 0 {
			t.Fatalf("len(results) = %d; want 0", len(results))
		}
	}

	waitGoroutines(t, base)
}

// TestStagesProgressConcurrently parks source 0 in the parse stage until
// source 1 has made it all the way through generate. A serial
// item-at-a-time implementation cannot satisfy this.
func TestStagesProgressConcurrently(t *testing.T) {
	genDone := make(chan struct{})
	const timeout = 5 * time.Second

	st := stages{
		parse: func(index int, src string) (*ast.Program, error) {
			if index == 0 {
				time.Sleep(20 * time.Millisecond)
				select {
				case <-genDone:
				case <-time.After(timeout):
					return nil, errors.New("generate stage did not run while parse was blocked")
				}
			}
			return parser.Parse(src)
		},
		bind: func(index int, prog *ast.Program) error {
			resolver.Resolve(prog)
			return nil
		},
		gen: func(index int, prog *ast.Program) (string, error) {
			out := generator.Generate(prog)
			if index == 1 {
				close(genDone)
			}
			return out, nil
		},
	}

	sources := []string{`const a = 1;`, `const b = 2;`, `const c = 3;`}
	base := runtime.NumGoroutine()

	done := make(chan []Result, 1)
	go func() { done <- run(sources, 3, st) }()

	select {
	case results := <-done:
		for i := range sources {
			if results[i].Err != nil {
				t.Fatalf("results[%d] failed: %v", i, results[i].Err)
			}
		}
	case <-time.After(timeout):
		t.Fatal("pipeline did not finish; stages did not run concurrently")
	}

	waitGoroutines(t, base)
}

func TestProcessLeavesNoGoroutines(t *testing.T) {
	base := runtime.NumGoroutine()

	sources := make([]string, 32)
	for i := range sources {
		sources[i] = "var y" + strconv.Itoa(i) + " = " + strconv.Itoa(i) + ";"
	}
	results := Process(sources, 4)
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("results[%d] failed: %v", i, r.Err)
		}
	}

	waitGoroutines(t, base)
}

// waitGoroutines waits for the goroutine count to settle back to want.
func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("goroutines did not exit: have %d, want <= %d", runtime.NumGoroutine(), want)
}

func TestStageString(t *testing.T) {
	tests := map[Stage]string{
		StageNone:     "none",
		StageParse:    "parse",
		StageBind:     "bind",
		StageGenerate: "generate",
	}
	for stage, want := range tests {
		if got := stage.String(); got != want {
			t.Errorf("Stage(%d).String() = %q; want %q", stage, got, want)
		}
	}
}

func TestResultOK(t *testing.T) {
	if !(Result{Source: "x"}).OK() {
		t.Error("Result with no error should be OK")
	}
	if (Result{Err: errors.New("boom"), Stage: StageBind}).OK() {
		t.Error("Result with an error should not be OK")
	}
}
