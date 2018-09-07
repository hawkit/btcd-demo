package btclog

import (
	"strings"
	"os"
	"io"
	"sync"
	"time"
	"bytes"
	"fmt"
	"runtime"
	"io/ioutil"
)

// defaultFlags specifies changes to the default logger behavior.  It is set
// during package init and configured using the LOGFLAGS environment variable.
// New logger backends can override these default flags using WithFlags.
var defaultFlags uint32

// Flags to modify Backend's behavior
const (
	// Llongfile modifies the logger output to include full path and line number
	// of the logging callsite, e.g. /a/b/c/main.go:123.
	Llongfile uint32 = 1 << iota

	// Lshortfile modifies the logger output to include filename and line number
	// of the logging callsite, e.g. mai.go:123. Overrides Llongfile
	Lshortfile
)

// Read logger flags from the LOGFLAGS environment variable. Multiple flags
// can be set at once, separated by commas.
func init() {
	for _, f := range strings.Split(os.Getenv("LOGFLAGS"), ",") {
		switch f {
		case "longfile":
			defaultFlags |= Llongfile
		case "shortfile":
			defaultFlags |= Lshortfile
		}
	}
}

// Level is the level at which a logger is configured. All messages sent
// to a level which is below the current level are filtered.
type Level uint32

// Level constants
const (
	LevelTrace Level = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelCritical
	LevelOff
)

// levelStrs defines the human-readable names for each logging level.
var levelStrs = [...]string{"TRC", "DBG", "INF", "WRN", "ERR", "CRT", "OFF"}


// LevelFromString returns a level based on the input string s.  If the input
// can't be interpreted as a valid log level, the info level and false is
// returned.
func LevelFromString(s string) (l Level, ok bool) {
	switch strings.ToLower(s) {
	case "trace", "trc":
		return LevelTrace, true
	case "debug", "dbg":
		return LevelDebug, true
	case "info", "inf":
		return LevelInfo, true
	case "warn", "wrn":
		return LevelWarn, true
	case "error", "err":
		return LevelError, true
	case "critical", "crt":
		return LevelCritical, true
	case "off":
		return LevelOff, true
	default:
		return LevelInfo, false
	}
}

// String returns the tag of the logger used in log messages, or "OFF" if
// the level will not produce any log output.
func (l Level) String() string {
	if l >= LevelOff {
		return "OFF"
	}
	return levelStrs[l]
}

// NewBackend creates a logger backend from a Writer
func NewBackend(w io.Writer, opts ...BackendOption) *Backend {
	b := &Backend{
		w:w,
		flag:defaultFlags,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Backend is a logging backend. Subsystems created from the backend write to
// the backend's Writer. Backend provides atomic writes to the Writer from all
// subsystems.
type Backend struct {
	w io.Writer
	mu sync.Mutex // ensures atomic writes
	flag uint32
}

// BackendOption is a function used to modify the behavior of a Backend.
type BackendOption func(b *Backend)

// WithFlags configures a Backend to use the specified flags rather than using
// the package's defaults as determind through the LOGFLAGS environment variable.
func WithFlags(flags uint32) BackendOption {
	return func(b *Backend) {
		b.flag = flags
	}
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 120)
		return &b
	},
}
// buffer returns a byte slice from the free list.  A new buffer is allocated if
// there are not any available on the free list. The returned byte slice should
// be returned to the free list by using the recycleBuffer function when then
// caller is done with it.
func buffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

// recycleBuffer puts the provided byte slice, which should have been obtain via
// the buffer function, back on the free list.
func recycleBuffer(b *[]byte) {
	*b = (*b)[:0]
	bufferPool.Put(b)
}

// From stdlib log package
// Cheap integer to fixed-width decimal ASCII. Give a negative width to avoid
// zero-padding.
func itoa(buf *[]byte, i int, wid int)  {
	// Assemble decimal in reverse order.
	var b [20]byte
	bp := len(b) - 1
	for  i > 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

// formatHeader appends a header in the default format 'YYYY-MM-DD hh:mm:ss.sss [LVL] TAG: '
// If either of the Lshortfile or lLongfile flags are specified, the file named
// and line number are included after the tag and before the final colon.
func formatHeader(buf *[]byte, t time.Time, lvl, tag string, file string, line int)  {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	ms := t.Nanosecond() / 1e6

	itoa(buf, year, 4)
	*buf = append(*buf, '-')
	itoa(buf, int(month), 2)
	*buf = append(*buf, '-')
	itoa(buf, int(day), 2)
	*buf = append(*buf, ' ')
	itoa(buf, hour, 2)
	*buf = append(*buf, ':')
	itoa(buf, min, 2)
	*buf = append(*buf, ':')
	itoa(buf, sec, 2)
	*buf = append(*buf, '.')
	itoa(buf, ms, 3)
	*buf = append(*buf, " ["...)
	*buf = append(*buf, lvl...)
	*buf = append(*buf, "] "...)
	*buf = append(*buf, tag...)

	if file != "" {
		*buf = append(*buf, ' ')
		*buf = append(*buf, file...)
		*buf = append(*buf, ':')
		itoa(buf, line, -1)
	}
	*buf = append(*buf, ": "...)
}

const defaultCallDepth = 3

func callsite(flag uint32) (string, int)  {
	_, file, line, ok := runtime.Caller(defaultCallDepth)
	if !ok {
		return "???", 0
	}
	if flag & Lshortfile != 0 {

		for i := len(file)-1; i >= 0; i-- {
			if os.IsPathSeparator(file[i]) {
				file = file[i+1:]
				break
			}
		}
	}
	return file, line

}
// print outputs a log message to the writer associated with the backend after
// creating a prefix for the given level and tag according to the formatHeader
// function and formatting the provided arguments using the default formatting
// rules
func (b *Backend) print(lvl, tag string, args ...interface{}) {
	t := time.Now() // get as early as possible

	bytebuff := buffer()
	defer recycleBuffer(bytebuff)

	var file string
	var line int
	if b.flag & (Llongfile | Lshortfile) != 0 {
		file, line = callsite(b.flag)
	}
	formatHeader(bytebuff, t, lvl, tag, file, line)
	buf := bytes.NewBuffer(*bytebuff)
	fmt.Fprintln(buf, args...)
	*bytebuff = buf.Bytes()

	b.mu.Lock()
	b.w.Write(*bytebuff)
	b.mu.Unlock()
}

func (b *Backend) printf(lvl, tag string, format string, args ...interface{}) {
	t := time.Now() // get as early as possible

	buff := buffer()
	defer recycleBuffer(buff)

	var file string
	var line int
	if b.flag & (Llongfile | Lshortfile) != 0 {
		file, line = callsite(b.flag)
	}
	formatHeader(buff, t, lvl, tag, file, line)
	newbuff := bytes.NewBuffer(*buff)
	fmt.Fprintf(newbuff, format, args...)
	*buff = append(newbuff.Bytes(), '\n')

	b.mu.Lock()
	b.w.Write(*buff)
	b.mu.Unlock()

}

func (b* Backend) Logger(subsystemTag string) Logger {
	return &slog{LevelInfo, subsystemTag, b}
}

type slog struct {
	lvl Level
	tag string
	b *Backend
}

func (l *slog) Tracef(format string, params ...interface{}) {
	if l.Level() <= LevelTrace {
		l.b.printf("TRC", l.tag, format, params...)
	}
}

func (l *slog) Debugf(format string, params ...interface{}) {
	if l.Level() <= LevelDebug {
		l.b.printf("DBG", l.tag, format, params...)
	}
}

func (l *slog) Infof(format string, params ...interface{}) {
	if l.Level() <= LevelInfo {
		l.b.printf("INF", l.tag, format, params...)
	}
}

func (l *slog) Warnf(format string, params ...interface{}) {
	if l.Level() <= LevelWarn {
		l.b.printf("WRN", l.tag, format, params...)
	}
}

func (l *slog) Errorf(format string, params ...interface{}) {
	if l.Level() <= LevelError {
		l.b.printf("ERR", l.tag, format, params...)
	}
}

func (l *slog) Criticalf(format string, params ...interface{}) {
	if l.Level() <= LevelCritical {
		l.b.printf("CRT", l.tag, format, params...)
	}
}

func (l *slog) Trace(v ...interface{}) {
	if l.Level() <= LevelTrace {
		l.b.print("TRC", l.tag, v...)
	}
}

func (l *slog) Debug(v ...interface{}) {
	if l.Level() <= LevelDebug {
		l.b.print("TRC", l.tag, v...)
	}
}

func (l *slog) Info(v ...interface{}) {
	if l.Level() <= LevelInfo {
		l.b.print("INF", l.tag, v...)
	}
}

func (l *slog) Warn(v ...interface{}) {
	if l.Level() <= LevelWarn {
		l.b.print("WRN", l.tag, v...)
	}
}

func (l *slog) Error(v ...interface{}) {
	if l.Level() <= LevelError {
		l.b.print("ERR", l.tag, v...)
	}
}

func (l *slog) Critical(v ...interface{}) {
	if l.Level() <= LevelCritical {
		l.b.print("CRT", l.tag, v...)
	}
}

func (l *slog) Level() Level {
	return l.lvl
}

func (l *slog) SetLevel(level Level) {
	l.lvl = level
}

// Disable is a logger that will never output anything.
var Disabled Logger

func init() {
	Disabled = &slog{lvl: LevelOff, b: NewBackend(ioutil.Discard)}
}







