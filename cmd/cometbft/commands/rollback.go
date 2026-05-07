package commands

import (
	"fmt"
	stdos "os"
	"path/filepath"

	"github.com/spf13/cobra"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
)

var (
	removeBlock = true
	numBlocks   = int64(1)
)

func init() {
	RollbackStateCmd.Flags().BoolVar(&removeBlock, "hard", true, "remove rolled-back blocks as well as state")
	RollbackStateCmd.Flags().Int64VarP(&numBlocks, "num", "n", 1, "number of blocks to rollback")
}

var RollbackStateCmd = &cobra.Command{
	Use:   "rollback",
	Short: "rollback CometBFT state by one or more heights",
	Long: `
A state rollback is performed to recover from an incorrect application state transition,
when CometBFT has persisted an incorrect app hash and is thus unable to make
progress. Rollback overwrites a state at height n with the state at height n - num.
The application should also roll back to the same height. By default, rolled-back
blocks are removed from the blockstore so they must be fetched or produced again.
For file private validators, hard rollback also rewinds the local signing state
to the rollback height. Use --hard=false to keep local blocks and replay them
against the rolled-back application state.
`,
	RunE: func(_ *cobra.Command, _ []string) error {
		height, hash, err := RollbackStateN(config, removeBlock, numBlocks)
		if err != nil {
			return fmt.Errorf("failed to rollback state: %w", err)
		}

		if removeBlock {
			fmt.Printf("Rolled back both state and block to height %d and hash %X\n", height, hash)
		} else {
			fmt.Printf("Rolled back state to height %d and hash %X\n", height, hash)
		}

		return nil
	},
}

// RollbackState takes the state at the current height n and overwrites it with the state
// at height n - 1. Note state here refers to CometBFT state not application state.
// Returns the latest state height and app hash alongside an error if there was one.
func RollbackState(config *cfg.Config, removeBlock bool) (int64, []byte, error) {
	return RollbackStateN(config, removeBlock, 1)
}

// RollbackStateN takes the state at the current height n and overwrites it with the state
// at height n - numBlocks. Note state here refers to CometBFT state not application state.
// Returns the latest state height and app hash alongside an error if there was one.
func RollbackStateN(config *cfg.Config, removeBlock bool, numBlocks int64) (int64, []byte, error) {
	// use the parsed config to load the block and state store
	blockStore, stateStore, err := loadStateAndBlockStore(config)
	if err != nil {
		return -1, nil, err
	}
	defer func() {
		_ = blockStore.Close()
		_ = stateStore.Close()
	}()

	height, hash, err := state.RollbackN(blockStore, stateStore, removeBlock, numBlocks)
	if err != nil {
		return -1, nil, err
	}

	if removeBlock {
		if err := rollbackFilePVState(config, height); err != nil {
			return -1, nil, err
		}
	}

	return height, hash, nil
}

func loadStateAndBlockStore(config *cfg.Config) (*store.BlockStore, state.Store, error) {
	dbType := dbm.BackendType(config.DBBackend)

	if !cmtos.FileExists(filepath.Join(config.DBDir(), "blockstore.db")) {
		return nil, nil, fmt.Errorf("no blockstore found in %v", config.DBDir())
	}

	// Get BlockStore
	blockStoreDB, err := dbm.NewDB("blockstore", dbType, config.DBDir())
	if err != nil {
		return nil, nil, err
	}
	blockStore := store.NewBlockStore(blockStoreDB, store.WithDBKeyLayout(config.Storage.ExperimentalKeyLayout))

	if !cmtos.FileExists(filepath.Join(config.DBDir(), "state.db")) {
		return nil, nil, fmt.Errorf("no statestore found in %v", config.DBDir())
	}

	// Get StateStore
	stateDB, err := dbm.NewDB("state", dbType, config.DBDir())
	if err != nil {
		return nil, nil, err
	}
	stateStore := state.NewStore(stateDB, state.StoreOptions{
		DiscardABCIResponses: config.Storage.DiscardABCIResponses,
	})

	return blockStore, stateStore, nil
}

func rollbackFilePVState(config *cfg.Config, height int64) error {
	keyFile := config.PrivValidatorKeyFile()
	stateFile := config.PrivValidatorStateFile()
	if _, err := stdos.Stat(keyFile); err != nil {
		if stdos.IsNotExist(err) {
			return nil
		}
		return err
	}
	if _, err := stdos.Stat(stateFile); err != nil {
		if stdos.IsNotExist(err) {
			return nil
		}
		return err
	}

	pv := privval.LoadFilePV(keyFile, stateFile)
	if pv.LastSignState.Height <= height {
		return nil
	}

	pv.LastSignState.Height = height
	pv.LastSignState.Round = 0
	pv.LastSignState.Step = 0
	pv.LastSignState.Signature = nil
	pv.LastSignState.SignBytes = nil
	pv.LastSignState.Save()
	return nil
}
