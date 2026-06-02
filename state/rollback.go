package state

import (
	"errors"
	"fmt"

	cmtstate "github.com/cometbft/cometbft/api/cometbft/state/v1"
	cmtversion "github.com/cometbft/cometbft/api/cometbft/version/v1"
	"github.com/cometbft/cometbft/version"
)

// Rollback overwrites the current CometBFT state (height n) with the most
// recent previous state (height n - 1).
// Note that this function does not affect application state.
func Rollback(bs BlockStore, ss Store, removeBlock bool) (int64, []byte, error) {
	return RollbackN(bs, ss, removeBlock, 1)
}

// RollbackN overwrites the current CometBFT state with the state that existed
// numBlocks blocks before the current blockstore height. When removeBlock is
// true, blockstore data above the rollback height is removed too.
// Note that this function does not affect application state.
func RollbackN(bs BlockStore, ss Store, removeBlock bool, numBlocks int64) (int64, []byte, error) {
	if numBlocks <= 0 {
		return -1, nil, fmt.Errorf("rollback block count must be greater than 0, got %d", numBlocks)
	}

	invalidState, err := ss.Load()
	if err != nil {
		return -1, nil, err
	}
	if invalidState.IsEmpty() {
		return -1, nil, errors.New("no state found")
	}

	blockHeight := bs.Height()

	// NOTE: persistence of state and blocks don't happen atomically. Therefore it is possible that
	// when the user stopped the node the state wasn't updated but the blockstore was. The blockstore
	// height may therefore be equal to or one above the statestore height.
	if blockHeight != invalidState.LastBlockHeight && blockHeight != invalidState.LastBlockHeight+1 {
		return -1, nil, fmt.Errorf("statestore height (%d) is not one below or equal to blockstore height (%d)",
			invalidState.LastBlockHeight, blockHeight)
	}

	rollbackHeight := blockHeight - numBlocks
	if rollbackHeight < invalidState.InitialHeight {
		return -1, nil, fmt.Errorf("cannot rollback %d blocks from height %d: target height %d is below initial height %d",
			numBlocks,
			blockHeight,
			rollbackHeight,
			invalidState.InitialHeight,
		)
	}

	if rollbackHeight == invalidState.LastBlockHeight {
		if removeBlock {
			if err := deleteBlocksAfter(bs, rollbackHeight); err != nil {
				return -1, nil, err
			}
		}
		return invalidState.LastBlockHeight, invalidState.AppHash, nil
	}

	if rollbackHeight > invalidState.LastBlockHeight {
		return -1, nil, fmt.Errorf("rollback target height (%d) is above statestore height (%d)",
			rollbackHeight,
			invalidState.LastBlockHeight,
		)
	}

	rollbackBlock := bs.LoadBlockMeta(rollbackHeight)
	if rollbackBlock == nil {
		return -1, nil, fmt.Errorf("block at height %d not found", rollbackHeight)
	}

	// We also need to retrieve the following block because the app hash and last
	// results hash for rollbackHeight are only agreed upon in the following block.
	nextBlock := bs.LoadBlockMeta(rollbackHeight + 1)
	if nextBlock == nil {
		return -1, nil, fmt.Errorf("block at height %d not found", rollbackHeight+1)
	}

	previousLastValidatorSet, err := ss.LoadValidators(rollbackHeight)
	if err != nil {
		return -1, nil, err
	}

	previousValidatorSet, err := ss.LoadValidators(rollbackHeight + 1)
	if err != nil {
		return -1, nil, err
	}

	previousNextValidatorSet, err := ss.LoadValidators(rollbackHeight + 2)
	if err != nil {
		return -1, nil, err
	}

	previousParams, err := ss.LoadConsensusParams(rollbackHeight + 1)
	if err != nil {
		return -1, nil, err
	}

	valChangeHeight, err := loadRollbackValidatorsChangeHeight(ss, invalidState, rollbackHeight)
	if err != nil {
		return -1, nil, err
	}

	paramsChangeHeight, err := loadRollbackConsensusParamsChangeHeight(ss, invalidState, rollbackHeight)
	if err != nil {
		return -1, nil, err
	}

	// build the new state from the old state and the prior block
	rolledBackState := State{
		Version: cmtstate.Version{
			Consensus: cmtversion.Consensus{
				Block: version.BlockProtocol,
				App:   previousParams.Version.App,
			},
			Software: version.CMTSemVer,
		},
		// immutable fields
		ChainID:       invalidState.ChainID,
		InitialHeight: invalidState.InitialHeight,

		LastBlockHeight: rollbackBlock.Header.Height,
		LastBlockID:     rollbackBlock.BlockID,
		LastBlockTime:   rollbackBlock.Header.Time,

		NextValidators:              previousNextValidatorSet,
		Validators:                  previousValidatorSet,
		LastValidators:              previousLastValidatorSet,
		LastHeightValidatorsChanged: valChangeHeight,

		ConsensusParams:                  previousParams,
		LastHeightConsensusParamsChanged: paramsChangeHeight,

		LastResultsHash: nextBlock.Header.LastResultsHash,
		AppHash:         nextBlock.Header.AppHash,
	}

	// persist the new state. This overrides the invalid one. NOTE: this will also
	// persist the validator set and consensus params over the existing structures,
	// but both should be the same
	if err := ss.Save(rolledBackState); err != nil {
		return -1, nil, fmt.Errorf("failed to save rolled back state: %w", err)
	}

	// If removeBlock is true then also remove blocks above the rollback height.
	// This will mean both the last state and last block height are equal to rollbackHeight.
	if removeBlock {
		if err := deleteBlocksAfter(bs, rollbackHeight); err != nil {
			return -1, nil, err
		}
	}

	return rolledBackState.LastBlockHeight, rolledBackState.AppHash, nil
}

type rollbackInfoStore interface {
	loadValidatorsChangeHeight(height int64) (int64, error)
	loadConsensusParamsChangeHeight(height int64) (int64, error)
}

func (store dbStore) loadValidatorsChangeHeight(height int64) (int64, error) {
	valInfo, _, err := loadValidatorsInfo(store.db, store.DBKeyLayout.CalcValidatorsKey(height))
	if err != nil {
		return 0, fmt.Errorf("could not find validators info for height %d: %w", height, err)
	}
	return valInfo.LastHeightChanged, nil
}

func (store dbStore) loadConsensusParamsChangeHeight(height int64) (int64, error) {
	paramsInfo, err := store.loadConsensusParamsInfo(height)
	if err != nil {
		return 0, fmt.Errorf("could not find consensus params info for height %d: %w", height, err)
	}
	return paramsInfo.LastHeightChanged, nil
}

func loadRollbackValidatorsChangeHeight(ss Store, invalidState State, rollbackHeight int64) (int64, error) {
	if infoStore, ok := ss.(rollbackInfoStore); ok {
		return infoStore.loadValidatorsChangeHeight(rollbackHeight + 2)
	}

	valChangeHeight := invalidState.LastHeightValidatorsChanged
	if valChangeHeight > rollbackHeight+2 {
		valChangeHeight = rollbackHeight + 2
	}
	return valChangeHeight, nil
}

func loadRollbackConsensusParamsChangeHeight(ss Store, invalidState State, rollbackHeight int64) (int64, error) {
	if infoStore, ok := ss.(rollbackInfoStore); ok {
		return infoStore.loadConsensusParamsChangeHeight(rollbackHeight + 1)
	}

	paramsChangeHeight := invalidState.LastHeightConsensusParamsChanged
	if paramsChangeHeight > rollbackHeight {
		paramsChangeHeight = rollbackHeight + 1
	}
	return paramsChangeHeight, nil
}

func deleteBlocksAfter(bs BlockStore, targetHeight int64) error {
	for bs.Height() > targetHeight {
		if err := bs.DeleteLatestBlock(); err != nil {
			return fmt.Errorf("failed to remove block %d from blockstore: %w", bs.Height(), err)
		}
	}
	return nil
}
