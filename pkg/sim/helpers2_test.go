package sim

import (
	"encoding/json"

	"github.com/lisama569/distributed-consensus-go/pkg/coordinator"
)

func acq(resource, holder, reqID string, leaseMS int64) coordinator.Command {
	return coordinator.Command{
		Kind:      coordinator.CmdAcquire,
		RequestID: reqID,
		Resource:  resource,
		Holder:    holder,
		LeaseMS:   leaseMS,
	}
}

func renew(resource, holder, reqID string, leaseMS int64) coordinator.Command {
	return coordinator.Command{
		Kind:      coordinator.CmdRenew,
		RequestID: reqID,
		Resource:  resource,
		Holder:    holder,
		LeaseMS:   leaseMS,
	}
}

func release(resource, holder, reqID string, token int64) coordinator.Command {
	return coordinator.Command{
		Kind:      coordinator.CmdRelease,
		RequestID: reqID,
		Resource:  resource,
		Holder:    holder,
		Token:     token,
	}
}

func commit(resource, holder, reqID string, token int64) coordinator.Command {
	return coordinator.Command{
		Kind:      coordinator.CmdCommit,
		RequestID: reqID,
		Resource:  resource,
		Holder:    holder,
		Token:     token,
	}
}

func encodeCmd(cmd coordinator.Command) ([]byte, error) { return json.Marshal(cmd) }
