// Package pipeline runs batches of JavaScript source through the
// parse → resolve → generate stages concurrently.
//
// A batch is spread across three stages connected by bounded channels, so
// different sources can be in different stages at the same time while the
// number of in-flight items stays bounded regardless of batch size.
package pipeline

import (
	"runtime"
	"sync"

	"github.com/t14raptor/go-fast/ast"
	"github.com/t14raptor/go-fast/generator"
	"github.com/t14raptor/go-fast/parser"
	"github.com/t14raptor/go-fast/resolver"
)

// Stage identifies a processing stage of the pipeline.
type Stage uint8

const (
	// StageNone marks a result that completed successfully.
	StageNone Stage = iota
	// StageParse is the parser.Parse stage.
	StageParse
	// StageBind is the resolver.Resolve stage.
	StageBind
	// StageGenerate is the generator.Generate stage.
	StageGenerate
)

// String returns the stage's name.
func (s Stage) String() string {
	switch s {
	case StageParse:
		return "parse"
	case StageBind:
		return "bind"
	case StageGenerate:
		return "generate"
	default:
		return "none"
	}
}

// Result is the outcome of processing one source.
//
// On success Err is nil and Source holds the generated JavaScript. On failure
// Err describes the failure, Stage names the stage that produced it, and
// Source is empty.
type Result struct {
	Source string
	Err    error
	Stage  Stage
}

// OK reports whether the source was processed successfully.
func (r Result) OK() bool { return r.Err == nil }

// work is one source travelling through the pipeline. Each item is owned by
// exactly one goroutine at a time, so work items share no mutable state.
type work struct {
	index int
	src   string
	prog  *ast.Program
}

// stages holds the per-stage operations. Tests substitute them to exercise
// failure and timing behavior; production always uses the defaults below.
type stages struct {
	parse func(index int, src string) (*ast.Program, error)
	bind  func(index int, prog *ast.Program) error
	gen   func(index int, prog *ast.Program) (string, error)
}

func defaultStages() stages {
	return stages{
		parse: func(_ int, src string) (*ast.Program, error) {
			return parser.Parse(src)
		},
		bind: func(_ int, prog *ast.Program) error {
			resolver.Resolve(prog)
			return nil
		},
		gen: func(_ int, prog *ast.Program) (string, error) {
			return generator.Generate(prog), nil
		},
	}
}

// defaultConcurrency is used when the caller passes a non-positive value.
func defaultConcurrency() int {
	if n := runtime.GOMAXPROCS(0); n > 0 {
		return n
	}
	return 1
}

// Process runs every source through parse, resolve and generate and returns
// one result per input, in input order.
//
// concurrency is the number of workers per stage; values less than one select
// a default based on GOMAXPROCS. A failure in one source is recorded at that
// source's index only and does not stop the rest of the batch.
func Process(sources []string, concurrency int) []Result {
	return run(sources, concurrency, defaultStages())
}

func run(sources []string, concurrency int, st stages) []Result {
	results := make([]Result, len(sources))
	if len(sources) == 0 {
		return results
	}

	if concurrency < 1 {
		concurrency = defaultConcurrency()
	}

	// Bounded channels cap how many ASTs may be buffered between stages at
	// once, which is what keeps a large batch from being parsed all at once.
	parseIn := make(chan *work, concurrency)
	bindIn := make(chan *work, concurrency)
	genIn := make(chan *work, concurrency)

	var parseWG, bindWG, genWG sync.WaitGroup

	startWorkers := func(n int, wg *sync.WaitGroup, fn func()) {
		wg.Add(n)
		for range n {
			go func() {
				defer wg.Done()
				fn()
			}()
		}
	}

	startWorkers(concurrency, &parseWG, func() {
		for item := range parseIn {
			prog, err := st.parse(item.index, item.src)
			if err != nil {
				results[item.index] = Result{Err: err, Stage: StageParse}
				continue
			}
			item.prog = prog
			bindIn <- item
		}
	})
	go func() {
		parseWG.Wait()
		close(bindIn)
	}()

	startWorkers(concurrency, &bindWG, func() {
		for item := range bindIn {
			if err := st.bind(item.index, item.prog); err != nil {
				results[item.index] = Result{Err: err, Stage: StageBind}
				continue
			}
			genIn <- item
		}
	})
	go func() {
		bindWG.Wait()
		close(genIn)
	}()

	startWorkers(concurrency, &genWG, func() {
		for item := range genIn {
			src, err := st.gen(item.index, item.prog)
			if err != nil {
				results[item.index] = Result{Err: err, Stage: StageGenerate}
				continue
			}
			results[item.index] = Result{Source: src}
		}
	})

	// Feed the first stage. This blocks once parseIn is full, which is the
	// backpressure that keeps the whole batch from being queued at once.
	for i, src := range sources {
		parseIn <- &work{index: i, src: src}
	}
	close(parseIn)

	genWG.Wait()
	parseWG.Wait()
	bindWG.Wait()
	return results
}
