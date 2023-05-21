package bitcask

const (
	DefaultSegmentSize = 256 * 1024 * 1024
)

var (
	DefaultOptions = &Options{
		Dir:         "db",
		SegmentSize: DefaultSegmentSize,
	}
)

type Options struct {
	Dir         string
	SegmentSize int64
}
