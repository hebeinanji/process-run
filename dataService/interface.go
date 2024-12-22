package dataService

import "context"

type DataService interface {
	CanRun(ctc context.Context) bool
	Run(ctx context.Context) bool
	FinishRun(ctx context.Context) (int64 bool)
}
