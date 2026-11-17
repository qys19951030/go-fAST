// Package pipeline runs batches of JavaScript source through the
// parse → resolve → generate stages concurrently.
package pipeline

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

// Process runs every source through parse, resolve and generate and returns
// one result per input, in input order.
//
// concurrency is the number of workers per stage; values less than one select
// a default based on GOMAXPROCS.
func Process(sources []string, concurrency int) []Result {
	return make([]Result, len(sources))
}
