package nethexec

import (
	"log/slog"
	"sync"
)

type comparisonWorkerPool struct {
	workers    chan struct{}
	logger     *slog.Logger
	bufferPool sync.Pool
}

func newComparisonWorkerPool(size int, logger *slog.Logger) *comparisonWorkerPool {
	return &comparisonWorkerPool{
		workers: make(chan struct{}, size),
		logger:  logger,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 0, 1024)
			},
		},
	}
}
