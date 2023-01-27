package rae

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/ratelimit"
)

type ClientWithTimeBasedRateLimit struct {
	rl ratelimit.Limiter
	ec *ethclient.Client
}

func (c *ClientWithTimeBasedRateLimit) Close() {
	c.ec.Close()
}

// ChainID retrieves the current chain ID for transaction replay protection.
func (c *ClientWithTimeBasedRateLimit) ChainID(ctx context.Context) (*big.Int, error) {
	c.rl.Take()
	return c.ec.ChainID(ctx)
}

// BlockByHash returns the given full block.
//
// Note that loading full blocks requires two requests. Use HeaderByHash
// if you don't need all transactions or uncle headers.
func (c *ClientWithTimeBasedRateLimit) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	c.rl.Take()
	return c.ec.BlockByHash(ctx, hash)
}

// BlockByNumber returns a block from the current canonical chain. If number is nil, the
// latest known block is returned.
//
// Note that loading full blocks requires two requests. Use HeaderByNumber
// if you don't need all transactions or uncle headers.
func (c *ClientWithTimeBasedRateLimit) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	c.rl.Take()
	return c.ec.BlockByNumber(ctx, number)
}

// BlockNumber returns the most recent block number
func (c *ClientWithTimeBasedRateLimit) BlockNumber(ctx context.Context) (uint64, error) {
	c.rl.Take()
	return c.ec.BlockNumber(ctx)
}

// PeerCount returns the number of p2p peers as reported by the net_peerCount method.
func (c *ClientWithTimeBasedRateLimit) PeerCount(ctx context.Context) (uint64, error) {
	c.rl.Take()
	return c.ec.PeerCount(ctx)
}

// HeaderByHash returns the block header with the given hash.
func (c *ClientWithTimeBasedRateLimit) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	c.rl.Take()
	return c.ec.HeaderByHash(ctx, hash)
}

// HeaderByNumber returns a block header from the current canonical chain. If number is
// nil, the latest known header is returned.
func (c *ClientWithTimeBasedRateLimit) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	c.rl.Take()
	return c.ec.HeaderByNumber(ctx, number)
}

// TransactionByHash returns the transaction with the given hash.
func (c *ClientWithTimeBasedRateLimit) TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error) {
	c.rl.Take()
	return c.ec.TransactionByHash(ctx, hash)
}

// TransactionSender returns the sender address of the given transaction. The transaction
// must be known to the remote node and included in the blockchain at the given block and
// index. The sender is the one derived by the protocol at the time of inclusion.
//
// There is a fast-path for transactions retrieved by TransactionByHash and
// TransactionInBlock. Getting their sender address can be done without an RPC interaction.
func (c *ClientWithTimeBasedRateLimit) TransactionSender(ctx context.Context, tx *types.Transaction, block common.Hash, index uint) (common.Address, error) {
	c.rl.Take()
	return c.ec.TransactionSender(ctx, tx, block, index)
}

// TransactionCount returns the total number of transactions in the given block.
func (c *ClientWithTimeBasedRateLimit) TransactionCount(ctx context.Context, blockHash common.Hash) (uint, error) {
	c.rl.Take()
	return c.ec.TransactionCount(ctx, blockHash)
}

// TransactionInBlock returns a single transaction at index in the given block.
func (c *ClientWithTimeBasedRateLimit) TransactionInBlock(ctx context.Context, blockHash common.Hash, index uint) (*types.Transaction, error) {
	c.rl.Take()
	return c.ec.TransactionInBlock(ctx, blockHash, index)
}

// TransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (c *ClientWithTimeBasedRateLimit) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	c.rl.Take()
	return c.ec.TransactionReceipt(ctx, txHash)
}

// SyncProgress retrieves the current progress of the sync algorithm. If there's
// no sync currently running, it returns nil.
func (c *ClientWithTimeBasedRateLimit) SyncProgress(ctx context.Context) (*ethereum.SyncProgress, error) {
	c.rl.Take()
	return c.ec.SyncProgress(ctx)
}

// SubscribeNewHead subscribes to notifications about the current blockchain head
// on the given channel.
func (c *ClientWithTimeBasedRateLimit) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	c.rl.Take()
	return c.ec.SubscribeNewHead(ctx, ch)
}

// State Access

// NetworkID returns the network ID (also known as the chain ID) for this chain.
func (c *ClientWithTimeBasedRateLimit) NetworkID(ctx context.Context) (*big.Int, error) {
	c.rl.Take()
	return c.ec.NetworkID(ctx)
}

// BalanceAt returns the wei balance of the given account.
// The block number can be nil, in which case the balance is taken from the latest known block.
func (c *ClientWithTimeBasedRateLimit) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	c.rl.Take()
	return c.ec.BalanceAt(ctx, account, blockNumber)
}

// StorageAt returns the value of key in the contract storage of the given account.
// The block number can be nil, in which case the value is taken from the latest known block.
func (c *ClientWithTimeBasedRateLimit) StorageAt(ctx context.Context, account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	c.rl.Take()
	return c.ec.StorageAt(ctx, account, key, blockNumber)
}

// CodeAt returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (c *ClientWithTimeBasedRateLimit) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	c.rl.Take()
	return c.ec.CodeAt(ctx, account, blockNumber)
}

// NonceAt returns the account nonce of the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (c *ClientWithTimeBasedRateLimit) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	c.rl.Take()
	return c.ec.NonceAt(ctx, account, blockNumber)
}

// Filters

// FilterLogs executes a filter query.
func (c *ClientWithTimeBasedRateLimit) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	c.rl.Take()
	return c.ec.FilterLogs(ctx, q)
}

// SubscribeFilterLogs subscribes to the results of a streaming filter query.
func (c *ClientWithTimeBasedRateLimit) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	c.rl.Take()
	return c.ec.SubscribeFilterLogs(ctx, q, ch)
}

// Pending State

// PendingBalanceAt returns the wei balance of the given account in the pending state.
func (c *ClientWithTimeBasedRateLimit) PendingBalanceAt(ctx context.Context, account common.Address) (*big.Int, error) {
	c.rl.Take()
	return c.ec.PendingBalanceAt(ctx, account)
}

// PendingStorageAt returns the value of key in the contract storage of the given account in the pending state.
func (c *ClientWithTimeBasedRateLimit) PendingStorageAt(ctx context.Context, account common.Address, key common.Hash) ([]byte, error) {
	c.rl.Take()
	return c.ec.PendingStorageAt(ctx, account, key)
}

// PendingCodeAt returns the contract code of the given account in the pending state.
func (c *ClientWithTimeBasedRateLimit) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	c.rl.Take()
	return c.ec.PendingCodeAt(ctx, account)
}

// PendingNonceAt returns the account nonce of the given account in the pending state.
// This is the nonce that should be used for the next transaction.
func (c *ClientWithTimeBasedRateLimit) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	c.rl.Take()
	return c.ec.PendingNonceAt(ctx, account)
}

// PendingTransactionCount returns the total number of transactions in the pending state.
func (c *ClientWithTimeBasedRateLimit) PendingTransactionCount(ctx context.Context) (uint, error) {
	c.rl.Take()
	return c.ec.PendingTransactionCount(ctx)
}

// Contract Calling

// CallContract executes a message call transaction, which is directly executed in the VM
// of the node, but never mined into the blockchain.
//
// blockNumber selects the block height at which the call runs. It can be nil, in which
// case the code is taken from the latest known block. Note that state from very old
// blocks might not be available.
func (c *ClientWithTimeBasedRateLimit) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	c.rl.Take()
	return c.ec.CallContract(ctx, msg, blockNumber)
}

// CallContractAtHash is almost the same as CallContract except that it selects
// the block by block hash instead of block height.
func (c *ClientWithTimeBasedRateLimit) CallContractAtHash(ctx context.Context, msg ethereum.CallMsg, blockHash common.Hash) ([]byte, error) {
	c.rl.Take()
	return c.ec.CallContractAtHash(ctx, msg, blockHash)
}

// PendingCallContract executes a message call transaction using the EVM.
// The state seen by the contract call is the pending state.
func (c *ClientWithTimeBasedRateLimit) PendingCallContract(ctx context.Context, msg ethereum.CallMsg) ([]byte, error) {
	c.rl.Take()
	return c.ec.PendingCallContract(ctx, msg)
}

// SuggestGasPrice retrieves the currently suggested gas price to allow a timely
// execution of a transaction.
func (c *ClientWithTimeBasedRateLimit) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	c.rl.Take()
	return c.ec.SuggestGasPrice(ctx)
}

// SuggestGasTipCap retrieves the currently suggested gas tip cap after 1559 to
// allow a timely execution of a transaction.
func (c *ClientWithTimeBasedRateLimit) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	c.rl.Take()
	return c.ec.SuggestGasTipCap(ctx)
}

// FeeHistory retrieves the fee market history.
func (c *ClientWithTimeBasedRateLimit) FeeHistory(ctx context.Context, blockCount uint64, lastBlock *big.Int, rewardPercentiles []float64) (*ethereum.FeeHistory, error) {
	return c.ec.FeeHistory(ctx, blockCount, lastBlock, rewardPercentiles)
}

// EstimateGas tries to estimate the gas needed to execute a specific transaction based on
// the current pending state of the backend blockchain. There is no guarantee that this is
// the true gas limit requirement as other transactions may be added or removed by miners,
// but it should provide a basis for setting a reasonable default.
func (c *ClientWithTimeBasedRateLimit) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	c.rl.Take()
	return c.ec.EstimateGas(ctx, msg)
}

// SendTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (c *ClientWithTimeBasedRateLimit) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	c.rl.Take()
	return c.ec.SendTransaction(ctx, tx)
}
