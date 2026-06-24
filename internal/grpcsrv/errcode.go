package grpcsrv

import (
	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connectToGRPC translates an error produced by a Connect service into a gRPC status error,
// preserving the code. Connect's Code constants share the same numeric values as gRPC's codes
// (Canceled=1 … Unauthenticated=16), so the mapping is a direct numeric cast. A nil error stays nil.
func connectToGRPC(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(codes.Code(connect.CodeOf(err)), err.Error())
}
