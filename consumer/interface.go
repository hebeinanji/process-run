package consumer

import "context"

type Consumer interface {
	RunConsumer(ctx context.Context) bool
}
