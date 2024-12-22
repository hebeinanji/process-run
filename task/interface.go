package task

import "context"

type Task interface {
	Producer(ctx context.Context) bool
}
