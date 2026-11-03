package abcicli

import (
	"context"
	"fmt"
	"sync"

	"github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/service"
)

type unsyncLocalClient struct {
	service.BaseService

	types.Application

	mtx sync.Mutex
	Callback

	// lastDelivered is closed once the callbacks of the most recent
	// CheckTxAsync request have been delivered. Each request waits for the
	// previous one's channel before delivering its own callbacks, so callbacks
	// run one at a time and in request order while the application checks run
	// in parallel. Guarded by mtx.
	lastDelivered chan struct{}
}

var _ Client = (*unsyncLocalClient)(nil)

// CodeTypeCheckTxFailed and CodespaceCheckTxFailed identify the CheckTx
// response of the unsync local client's CheckTxAsync when the application
// returned an error or panicked.
const (
	CodeTypeCheckTxFailed  uint32 = 1
	CodespaceCheckTxFailed        = "abci-client"
)

// NewUnsyncLocalClient creates a local client, which wraps the application
// interface that Comet as the client will call to the application as the
// server.
//
// This differs from [NewLocalClient] in that it returns a client that only
// maintains a mutex over the callback used by CheckTxAsync and not over the
// application, leaving it up to the proxy to handle all concurrency. If the
// proxy does not impose any concurrency restrictions, it is then left up to
// the application to implement its own concurrency for the relevant group of
// calls.
func NewUnsyncLocalClient(app types.Application) Client {
	cli := &unsyncLocalClient{
		Application: app,
	}
	cli.BaseService = *service.NewBaseService(nil, "unsyncLocalClient", cli)
	return cli
}

func (app *unsyncLocalClient) SetResponseCallback(cb Callback) {
	app.mtx.Lock()
	app.Callback = cb
	app.mtx.Unlock()
}

// CheckTxAsync runs CheckTx on the application in a new goroutine and returns
// immediately. Application calls of consecutive requests run in parallel, but
// their callbacks are delivered serially and in request order, as with the
// other ABCI clients. Flush waits until all callbacks have been delivered.
func (app *unsyncLocalClient) CheckTxAsync(ctx context.Context, req *types.CheckTxRequest) (*ReqRes, error) {
	reqres := NewReqRes(types.ToCheckTxRequest(req))

	delivered := make(chan struct{})
	app.mtx.Lock()
	prevDelivered := app.lastDelivered
	app.lastDelivered = delivered
	app.mtx.Unlock()

	go func() {
		defer close(delivered)

		reqres.Response = types.ToCheckTxResponse(app.checkTx(ctx, req))

		if prevDelivered != nil {
			<-prevDelivered
		}

		app.mtx.Lock()
		cb := app.Callback
		app.mtx.Unlock()
		if cb != nil {
			cb(reqres.Request, reqres.Response)
		}

		// Invoke the callback before releasing waiters, so that they see its
		// result in reqres.Error().
		reqres.InvokeCallback()
		reqres.Done()
	}()

	return reqres, nil
}

// checkTx calls CheckTx on the application. An error or a panic is reported
// as a rejected tx: the mempool callbacks expect a CheckTx response, and a
// panic would crash the node, since nothing up the stack of the CheckTxAsync
// goroutine recovers it.
func (app *unsyncLocalClient) checkTx(ctx context.Context, req *types.CheckTxRequest) (res *types.CheckTxResponse) {
	defer func() {
		if r := recover(); r != nil {
			res = checkTxFailed(fmt.Errorf("CheckTx panicked: %v", r))
		}
	}()

	res, err := app.Application.CheckTx(ctx, req)
	if err != nil {
		return checkTxFailed(err)
	}
	return res
}

func checkTxFailed(err error) *types.CheckTxResponse {
	return &types.CheckTxResponse{
		Code:      CodeTypeCheckTxFailed,
		Log:       err.Error(),
		Codespace: CodespaceCheckTxFailed,
	}
}

// -------------------------------------------------------

func (*unsyncLocalClient) Error() error {
	return nil
}

// Flush blocks until the callbacks of all CheckTxAsync requests issued so far
// have been delivered.
func (app *unsyncLocalClient) Flush(ctx context.Context) error {
	app.mtx.Lock()
	lastDelivered := app.lastDelivered
	app.mtx.Unlock()

	if lastDelivered == nil {
		return nil
	}
	select {
	case <-lastDelivered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*unsyncLocalClient) Echo(_ context.Context, msg string) (*types.EchoResponse, error) {
	return &types.EchoResponse{Message: msg}, nil
}

func (app *unsyncLocalClient) Info(ctx context.Context, req *types.InfoRequest) (*types.InfoResponse, error) {
	return app.Application.Info(ctx, req)
}

func (app *unsyncLocalClient) CheckTx(ctx context.Context, req *types.CheckTxRequest) (*types.CheckTxResponse, error) {
	return app.Application.CheckTx(ctx, req)
}

func (app *unsyncLocalClient) Query(ctx context.Context, req *types.QueryRequest) (*types.QueryResponse, error) {
	return app.Application.Query(ctx, req)
}

func (app *unsyncLocalClient) Commit(ctx context.Context, req *types.CommitRequest) (*types.CommitResponse, error) {
	return app.Application.Commit(ctx, req)
}

func (app *unsyncLocalClient) InitChain(ctx context.Context, req *types.InitChainRequest) (*types.InitChainResponse, error) {
	return app.Application.InitChain(ctx, req)
}

func (app *unsyncLocalClient) ListSnapshots(ctx context.Context, req *types.ListSnapshotsRequest) (*types.ListSnapshotsResponse, error) {
	return app.Application.ListSnapshots(ctx, req)
}

func (app *unsyncLocalClient) OfferSnapshot(ctx context.Context, req *types.OfferSnapshotRequest) (*types.OfferSnapshotResponse, error) {
	return app.Application.OfferSnapshot(ctx, req)
}

func (app *unsyncLocalClient) LoadSnapshotChunk(ctx context.Context,
	req *types.LoadSnapshotChunkRequest,
) (*types.LoadSnapshotChunkResponse, error) {
	return app.Application.LoadSnapshotChunk(ctx, req)
}

func (app *unsyncLocalClient) ApplySnapshotChunk(ctx context.Context,
	req *types.ApplySnapshotChunkRequest,
) (*types.ApplySnapshotChunkResponse, error) {
	return app.Application.ApplySnapshotChunk(ctx, req)
}

func (app *unsyncLocalClient) PrepareProposal(ctx context.Context, req *types.PrepareProposalRequest) (*types.PrepareProposalResponse, error) {
	return app.Application.PrepareProposal(ctx, req)
}

func (app *unsyncLocalClient) ProcessProposal(ctx context.Context, req *types.ProcessProposalRequest) (*types.ProcessProposalResponse, error) {
	return app.Application.ProcessProposal(ctx, req)
}

func (app *unsyncLocalClient) ExtendVote(ctx context.Context, req *types.ExtendVoteRequest) (*types.ExtendVoteResponse, error) {
	return app.Application.ExtendVote(ctx, req)
}

func (app *unsyncLocalClient) VerifyVoteExtension(ctx context.Context, req *types.VerifyVoteExtensionRequest) (*types.VerifyVoteExtensionResponse, error) {
	return app.Application.VerifyVoteExtension(ctx, req)
}

func (app *unsyncLocalClient) FinalizeBlock(ctx context.Context, req *types.FinalizeBlockRequest) (*types.FinalizeBlockResponse, error) {
	return app.Application.FinalizeBlock(ctx, req)
}
