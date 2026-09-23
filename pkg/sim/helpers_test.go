package sim

import "github.com/lisama569/distributed-consensus-go/pkg/coordinator"

func mustAcquire(resource, holder, reqID string, leaseMS int64) coordinator.Command {
	return coordinator.Command{
		Kind:      coordinator.CmdAcquire,
		RequestID: reqID,
		Resource:  resource,
		Holder:    holder,
		LeaseMS:   leaseMS,
	}
}
