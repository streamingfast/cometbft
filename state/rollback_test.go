package state_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cmtstate "github.com/cometbft/cometbft/api/cometbft/state/v1"
	cmtversion "github.com/cometbft/cometbft/api/cometbft/version/v1"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/tmhash"
	"github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/mocks"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/cometbft/cometbft/version"
)

func TestRollback(t *testing.T) {
	var (
		height     int64 = 100
		nextHeight int64 = 101
	)
	blockStore := &mocks.BlockStore{}
	stateStore := setupStateStore(t, height)
	initialState, err := stateStore.Load()
	require.NoError(t, err)

	// perform the rollback over a version bump
	newParams := types.DefaultConsensusParams()
	newParams.Version.App = 11
	newParams.Block.MaxBytes = 1000
	nextState := initialState.Copy()
	nextState.LastBlockHeight = nextHeight
	nextState.Version.Consensus.App = 11
	nextState.LastBlockID = makeBlockIDRandom()
	nextState.AppHash = tmhash.Sum([]byte("app_hash"))
	nextState.LastValidators = initialState.Validators
	nextState.Validators = initialState.NextValidators
	nextState.NextValidators = initialState.NextValidators.CopyIncrementProposerPriority(1)
	nextState.ConsensusParams = *newParams
	nextState.LastHeightConsensusParamsChanged = nextHeight + 1
	nextState.LastHeightValidatorsChanged = nextHeight + 1

	// update the state
	require.NoError(t, stateStore.Save(nextState))

	block := &types.BlockMeta{
		BlockID: initialState.LastBlockID,
		Header: types.Header{
			Height:          initialState.LastBlockHeight,
			Time:            initialState.LastBlockTime,
			AppHash:         crypto.CRandBytes(tmhash.Size),
			LastBlockID:     makeBlockIDRandom(),
			LastResultsHash: initialState.LastResultsHash,
		},
	}
	nextBlock := &types.BlockMeta{
		BlockID: initialState.LastBlockID,
		Header: types.Header{
			Height:          nextState.LastBlockHeight,
			AppHash:         initialState.AppHash,
			LastBlockID:     block.BlockID,
			Time:            nextState.LastBlockTime,
			LastResultsHash: nextState.LastResultsHash,
		},
	}
	blockStore.On("LoadBlockMeta", height).Return(block)
	blockStore.On("LoadBlockMeta", nextHeight).Return(nextBlock)
	blockStore.On("Height").Return(nextHeight)

	// rollback the state
	rollbackHeight, rollbackHash, err := state.Rollback(blockStore, stateStore, false)
	require.NoError(t, err)
	require.EqualValues(t, height, rollbackHeight)
	require.EqualValues(t, initialState.AppHash, rollbackHash)
	blockStore.AssertExpectations(t)

	// assert that we've recovered the prior state
	loadedState, err := stateStore.Load()
	require.NoError(t, err)
	require.EqualValues(t, initialState, loadedState)
}

func TestRollbackHard(t *testing.T) {
	const height int64 = 100
	blockStore := store.NewBlockStore(dbm.NewMemDB())
	stateStore := state.NewStore(dbm.NewMemDB(), state.StoreOptions{DiscardABCIResponses: false})

	valSet, _ := types.RandValidatorSet(5, 10)

	params := types.DefaultConsensusParams()
	params.Version.App = 10
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	block := &types.Block{
		Header: types.Header{
			Version:            cmtversion.Consensus{Block: version.BlockProtocol, App: 1},
			ChainID:            "test-chain",
			Time:               now,
			Height:             height,
			AppHash:            crypto.CRandBytes(tmhash.Size),
			LastBlockID:        makeBlockIDRandom(),
			LastCommitHash:     crypto.CRandBytes(tmhash.Size),
			DataHash:           crypto.CRandBytes(tmhash.Size),
			ValidatorsHash:     valSet.Hash(),
			NextValidatorsHash: valSet.CopyIncrementProposerPriority(1).Hash(),
			ConsensusHash:      params.Hash(),
			LastResultsHash:    crypto.CRandBytes(tmhash.Size),
			EvidenceHash:       crypto.CRandBytes(tmhash.Size),
			ProposerAddress:    crypto.CRandBytes(crypto.AddressSize),
		},
		LastCommit: &types.Commit{Height: height - 1},
	}

	partSet, err := block.MakePartSet(types.BlockPartSizeBytes)
	require.NoError(t, err)
	blockStore.SaveBlock(block, partSet, &types.Commit{Height: block.Height})

	currState := state.State{
		Version: cmtstate.Version{
			Consensus: block.Header.Version,
			Software:  version.CMTSemVer,
		},
		LastBlockHeight:                  block.Height,
		LastBlockTime:                    block.Time,
		AppHash:                          crypto.CRandBytes(tmhash.Size),
		LastValidators:                   valSet,
		Validators:                       valSet.CopyIncrementProposerPriority(1),
		NextValidators:                   valSet.CopyIncrementProposerPriority(2),
		ConsensusParams:                  *params,
		LastHeightConsensusParamsChanged: height + 1,
		LastHeightValidatorsChanged:      height + 1,
		LastResultsHash:                  crypto.CRandBytes(tmhash.Size),
	}
	require.NoError(t, stateStore.Bootstrap(currState))

	nextBlock := &types.Block{
		Header: types.Header{
			Version:            cmtversion.Consensus{Block: version.BlockProtocol, App: 1},
			ChainID:            block.ChainID,
			Time:               block.Time,
			Height:             currState.LastBlockHeight + 1,
			AppHash:            currState.AppHash,
			LastBlockID:        types.BlockID{Hash: block.Hash(), PartSetHeader: partSet.Header()},
			LastCommitHash:     crypto.CRandBytes(tmhash.Size),
			DataHash:           crypto.CRandBytes(tmhash.Size),
			ValidatorsHash:     valSet.CopyIncrementProposerPriority(1).Hash(),
			NextValidatorsHash: valSet.CopyIncrementProposerPriority(2).Hash(),
			ConsensusHash:      params.Hash(),
			LastResultsHash:    currState.LastResultsHash,
			EvidenceHash:       crypto.CRandBytes(tmhash.Size),
			ProposerAddress:    crypto.CRandBytes(crypto.AddressSize),
		},
		LastCommit: &types.Commit{Height: currState.LastBlockHeight},
	}

	nextPartSet, err := nextBlock.MakePartSet(types.BlockPartSizeBytes)
	require.NoError(t, err)
	blockStore.SaveBlock(nextBlock, nextPartSet, &types.Commit{Height: nextBlock.Height})

	rollbackHeight, rollbackHash, err := state.Rollback(blockStore, stateStore, true)
	require.NoError(t, err)
	require.Equal(t, rollbackHeight, currState.LastBlockHeight)
	require.Equal(t, rollbackHash, currState.AppHash)

	// state should not have been changed
	loadedState, err := stateStore.Load()
	require.NoError(t, err)
	require.Equal(t, currState, loadedState)

	// resave the same block
	blockStore.SaveBlock(nextBlock, nextPartSet, &types.Commit{Height: nextBlock.Height})

	params.Version.App = 11

	nextState := state.State{
		Version: cmtstate.Version{
			Consensus: block.Header.Version,
			Software:  version.CMTSemVer,
		},
		LastBlockHeight:                  nextBlock.Height,
		LastBlockTime:                    nextBlock.Time,
		AppHash:                          crypto.CRandBytes(tmhash.Size),
		LastValidators:                   valSet.CopyIncrementProposerPriority(1),
		Validators:                       valSet.CopyIncrementProposerPriority(2),
		NextValidators:                   valSet.CopyIncrementProposerPriority(3),
		ConsensusParams:                  *params,
		LastHeightConsensusParamsChanged: nextBlock.Height + 1,
		LastHeightValidatorsChanged:      nextBlock.Height + 1,
		LastResultsHash:                  crypto.CRandBytes(tmhash.Size),
	}
	require.NoError(t, stateStore.Save(nextState))

	rollbackHeight, rollbackHash, err = state.Rollback(blockStore, stateStore, true)
	require.NoError(t, err)
	require.Equal(t, rollbackHeight, currState.LastBlockHeight)
	require.Equal(t, rollbackHash, currState.AppHash)
}

func TestRollbackNHard(t *testing.T) {
	const (
		chainHeight  int64 = 20
		targetHeight int64 = 10
	)

	blockStore := store.NewBlockStore(dbm.NewMemDB())
	stateStore := state.NewStore(dbm.NewMemDB(), state.StoreOptions{DiscardABCIResponses: false})
	states := setupRollbackNChain(t, blockStore, stateStore, chainHeight)

	rollbackHeight, rollbackHash, err := state.RollbackN(blockStore, stateStore, true, chainHeight-targetHeight)
	require.NoError(t, err)
	require.Equal(t, targetHeight, rollbackHeight)
	require.Equal(t, states[targetHeight].AppHash, rollbackHash)
	require.Equal(t, targetHeight, blockStore.Height())
	require.NotNil(t, blockStore.LoadBlockMeta(targetHeight))
	require.Nil(t, blockStore.LoadBlockMeta(targetHeight+1))

	loadedState, err := stateStore.Load()
	require.NoError(t, err)
	require.Equal(t, states[targetHeight], loadedState)
}

func TestRollbackNSoftKeepsBlocks(t *testing.T) {
	const (
		chainHeight  int64 = 20
		targetHeight int64 = 15
	)

	blockStore := store.NewBlockStore(dbm.NewMemDB())
	stateStore := state.NewStore(dbm.NewMemDB(), state.StoreOptions{DiscardABCIResponses: false})
	states := setupRollbackNChain(t, blockStore, stateStore, chainHeight)

	rollbackHeight, rollbackHash, err := state.RollbackN(blockStore, stateStore, false, chainHeight-targetHeight)
	require.NoError(t, err)
	require.Equal(t, targetHeight, rollbackHeight)
	require.Equal(t, states[targetHeight].AppHash, rollbackHash)
	require.Equal(t, chainHeight, blockStore.Height())
	require.NotNil(t, blockStore.LoadBlockMeta(chainHeight))

	loadedState, err := stateStore.Load()
	require.NoError(t, err)
	require.Equal(t, states[targetHeight], loadedState)
}

func TestRollbackNoState(t *testing.T) {
	stateStore := state.NewStore(dbm.NewMemDB(),
		state.StoreOptions{
			DiscardABCIResponses: false,
		})
	blockStore := &mocks.BlockStore{}

	_, _, err := state.Rollback(blockStore, stateStore, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no state found")
}

func TestRollbackNoBlocks(t *testing.T) {
	const height = int64(100)
	stateStore := setupStateStore(t, height)
	blockStore := &mocks.BlockStore{}
	blockStore.On("Height").Return(height)
	blockStore.On("LoadBlockMeta", height).Return(nil)
	blockStore.On("LoadBlockMeta", height-1).Return(nil)

	_, _, err := state.Rollback(blockStore, stateStore, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block at height 99 not found")
}

func TestRollbackDifferentStateHeight(t *testing.T) {
	const height = int64(100)
	stateStore := setupStateStore(t, height)
	blockStore := &mocks.BlockStore{}
	blockStore.On("Height").Return(height + 2)

	_, _, err := state.Rollback(blockStore, stateStore, false)
	require.Error(t, err)
	require.Equal(t, "statestore height (100) is not one below or equal to blockstore height (102)", err.Error())
}

func setupStateStore(t *testing.T, height int64) state.Store {
	t.Helper()
	stateStore := state.NewStore(dbm.NewMemDB(), state.StoreOptions{DiscardABCIResponses: false})
	valSet, _ := types.RandValidatorSet(5, 10)

	params := types.DefaultConsensusParams()
	params.Version.App = 10

	initialState := state.State{
		Version: cmtstate.Version{
			Consensus: cmtversion.Consensus{
				Block: version.BlockProtocol,
				App:   10,
			},
			Software: version.CMTSemVer,
		},
		ChainID:                          "test-chain",
		InitialHeight:                    10,
		LastBlockID:                      makeBlockIDRandom(),
		AppHash:                          tmhash.Sum([]byte("app_hash")),
		LastResultsHash:                  tmhash.Sum([]byte("last_results_hash")),
		LastBlockHeight:                  height,
		LastBlockTime:                    cmttime.Now(),
		LastValidators:                   valSet,
		Validators:                       valSet.CopyIncrementProposerPriority(1),
		NextValidators:                   valSet.CopyIncrementProposerPriority(2),
		LastHeightValidatorsChanged:      height + 1 + 1,
		ConsensusParams:                  *params,
		LastHeightConsensusParamsChanged: height + 1,
	}
	require.NoError(t, stateStore.Bootstrap(initialState))
	return stateStore
}

func setupRollbackNChain(t *testing.T, blockStore *store.BlockStore, stateStore state.Store, chainHeight int64) map[int64]state.State {
	t.Helper()

	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	valSet, _ := types.RandValidatorSet(4, 10)
	params := types.DefaultConsensusParams()
	params.Version.App = 1

	currState := state.State{
		Version: cmtstate.Version{
			Consensus: cmtversion.Consensus{
				Block: version.BlockProtocol,
				App:   params.Version.App,
			},
			Software: version.CMTSemVer,
		},
		ChainID:                          "rollback-n-test-chain",
		InitialHeight:                    1,
		LastBlockTime:                    now,
		Validators:                       valSet,
		NextValidators:                   valSet.CopyIncrementProposerPriority(1),
		LastValidators:                   types.NewValidatorSet(nil),
		LastHeightValidatorsChanged:      1,
		ConsensusParams:                  *params,
		LastHeightConsensusParamsChanged: 1,
		LastResultsHash:                  tmhash.Sum([]byte("results_0")),
		AppHash:                          tmhash.Sum([]byte("app_hash_0")),
	}
	require.NoError(t, stateStore.Bootstrap(currState))

	states := make(map[int64]state.State, chainHeight)
	for height := int64(1); height <= chainHeight; height++ {
		block := &types.Block{
			Header: types.Header{
				Version:            currState.Version.Consensus,
				ChainID:            currState.ChainID,
				Time:               now.Add(time.Duration(height) * time.Second),
				Height:             height,
				AppHash:            currState.AppHash,
				LastBlockID:        currState.LastBlockID,
				ValidatorsHash:     currState.Validators.Hash(),
				NextValidatorsHash: currState.NextValidators.Hash(),
				ConsensusHash:      currState.ConsensusParams.Hash(),
				LastResultsHash:    currState.LastResultsHash,
				ProposerAddress:    currState.Validators.Proposer.Address,
			},
			LastCommit: &types.Commit{Height: height - 1},
		}
		partSet, err := block.MakePartSet(types.BlockPartSizeBytes)
		require.NoError(t, err)
		blockStore.SaveBlock(block, partSet, &types.Commit{Height: height})
		blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: partSet.Header()}

		nextValidators := currState.NextValidators.Copy()
		lastHeightValidatorsChanged := currState.LastHeightValidatorsChanged
		if height == 7 || height == 15 {
			newValidator, _ := types.RandValidator(false, 10)
			require.NoError(t, nextValidators.UpdateWithChangeSet([]*types.Validator{newValidator}))
			lastHeightValidatorsChanged = height + 2
		}
		nextValidators.IncrementProposerPriority(1)

		nextParams := currState.ConsensusParams
		lastHeightConsensusParamsChanged := currState.LastHeightConsensusParamsChanged
		nextVersion := currState.Version
		if height == 7 || height == 15 {
			nextParams.Block.MaxBytes += height
			nextParams.Version.App++
			nextVersion.Consensus.App = nextParams.Version.App
			lastHeightConsensusParamsChanged = height + 1
		}

		currState = state.State{
			Version:                          nextVersion,
			ChainID:                          currState.ChainID,
			InitialHeight:                    currState.InitialHeight,
			LastBlockHeight:                  height,
			LastBlockID:                      blockID,
			LastBlockTime:                    block.Time,
			NextValidators:                   nextValidators,
			Validators:                       currState.NextValidators.Copy(),
			LastValidators:                   currState.Validators.Copy(),
			LastHeightValidatorsChanged:      lastHeightValidatorsChanged,
			ConsensusParams:                  nextParams,
			LastHeightConsensusParamsChanged: lastHeightConsensusParamsChanged,
			LastResultsHash:                  tmhash.Sum([]byte(fmt.Sprintf("results_%d", height))),
			AppHash:                          tmhash.Sum([]byte(fmt.Sprintf("app_hash_%d", height))),
		}
		require.NoError(t, stateStore.Save(currState))
		states[height] = currState
	}

	return states
}

func makeBlockIDRandom() types.BlockID {
	var (
		blockHash   = make([]byte, tmhash.Size)
		partSetHash = make([]byte, tmhash.Size)
	)
	rand.Read(blockHash)   //nolint: errcheck // ignore errcheck for read
	rand.Read(partSetHash) //nolint: errcheck // ignore errcheck for read
	return types.BlockID{
		Hash: blockHash,
		PartSetHeader: types.PartSetHeader{
			Total: 123,
			Hash:  partSetHash,
		},
	}
}
