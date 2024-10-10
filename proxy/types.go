package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
)

// Note on optional Signer field:
// * when receiving from Flashbots or other builders this field should be set
// * otherwise its set from the request signature by orderflow proxy
//   in this case it can be empty! @should we prohibit that?

// eth_SendBundle

type EthSendBundleArgs struct {
	Txs               []hexutil.Bytes `json:"txs"`         // empty txs for cancellations are not supported
	BlockNumber       rpc.BlockNumber `json:"blockNumber"` // 0 block number is not supported
	MinTimestamp      *uint64         `json:"minTimestamp,omitempty"`
	MaxTimestamp      *uint64         `json:"maxTimestamp,omitempty"`
	RevertingTxHashes []common.Hash   `json:"revertingTxHashes,omitempty"`
	ReplacementUUID   *string         `json:"replacementUuid,omitempty"`

	// fields available only when receiving from the Flashbots or other builders, not users
	ReplacementNonce *uint64         `json:"replacementNonce,omitempty"`
	SigningAddress   *common.Address `json:"signingAddress,omitempty"`

	DroppingTxHashes []common.Hash   `json:"droppingTxHashes,omitempty"` // not supported (from beaverbuild)
	UUID             *string         `json:"uuid,omitempty"`             // not supported (from beaverbuild)
	RefundPercent    *uint64         `json:"refundPercent,omitempty"`    // not supported (from beaverbuild)
	RefundRecipient  *common.Address `json:"refundRecipient,omitempty"`  // not supported (from beaverbuild)
	RefundTxHashes   []string        `json:"refundTxHashes,omitempty"`   // not supported (from titanbuilder)
}

// mev_sendBundle

const (
	RevertModeAllow = "allow"
	RevertModeDrop  = "drop"
	RevertModeFail  = "fail"
)

type MevBundleInclusion struct {
	BlockNumber hexutil.Uint64 `json:"block"`
	MaxBlock    hexutil.Uint64 `json:"maxBlock"`
}

type MevBundleBody struct {
	Hash       *common.Hash       `json:"hash,omitempty"`
	Tx         *hexutil.Bytes     `json:"tx,omitempty"`
	Bundle     *MevSendBundleArgs `json:"bundle,omitempty"`
	CanRevert  bool               `json:"canRevert,omitempty"`
	RevertMode string             `json:"revertMode,omitempty"`
}

type MevBundleValidity struct {
	Refund       []RefundConstraint `json:"refund,omitempty"`
	RefundConfig []RefundConfig     `json:"refundConfig,omitempty"`
}

type RefundConstraint struct {
	BodyIdx int `json:"bodyIdx"`
	Percent int `json:"percent"`
}

type RefundConfig struct {
	Address common.Address `json:"address"`
	Percent int            `json:"percent"`
}

type MevBundleMetadata struct {
	// should be set only if we are receiving from Flashbots or other builders
	Signer *common.Address `json:"signer,omitempty"`
}

type MevSendBundleArgs struct {
	Version         string             `json:"version"`
	ReplacementUUID string             `json:"replacementUuid,omitempty"`
	Inclusion       MevBundleInclusion `json:"inclusion"`
	// when empty its considered cancel
	Body     []MevBundleBody    `json:"body"`
	Validity MevBundleValidity  `json:"validity"`
	Metadata *MevBundleMetadata `json:"metadata,omitempty"`

	// must be empty
	Privacy *json.RawMessage `json:"privacy,omitempty"`
}

// eth_sendRawTransaction

type EthSendRawTransactionArgs hexutil.Bytes

// eth_cancelBundle

type EthCancelBundleArgs struct {
	// @todo can builder accept camel case?
	// this type seems incompatible between builder and other infra
	ReplacementUUID string          `json:"replacementUuid"`
	SigningAddress  *common.Address `json:"signingAddress"`
}

// bid_subsidiseBlock

type BidSubsisideBlockArgs uint64

/// unique key
/// unique key is used to deduplicate requests, its will give different resuts then bundle uuid

func newHash() hash.Hash {
	return sha256.New()
}

func uuidFromHash(h hash.Hash) uuid.UUID {
	version := 5
	s := h.Sum(nil)
	var uuid uuid.UUID
	copy(uuid[:], s)
	uuid[6] = (uuid[6] & 0x0f) | uint8((version&0xf)<<4)
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // RFC 4122 variant
	return uuid
}

func (b *EthSendBundleArgs) UniqueKey() uuid.UUID {
	hash := newHash()
	_ = binary.Write(hash, binary.LittleEndian, b.BlockNumber.Int64())
	for _, tx := range b.Txs {
		_, _ = hash.Write(tx)
	}
	sort.Slice(b.RevertingTxHashes, func(i, j int) bool {
		return bytes.Compare(b.RevertingTxHashes[i][:], b.RevertingTxHashes[j][:]) <= 0
	})
	for _, txHash := range b.RevertingTxHashes {
		_, _ = hash.Write(txHash.Bytes())
	}
	_, _ = hash.Write(b.SigningAddress.Bytes())
	return uuidFromHash(hash)
}

func (b *MevSendBundleArgs) UniqueKey() uuid.UUID {
	hash := newHash()
	uniqueKeyMevSendBundle(b, hash)
	return uuidFromHash(hash)
}

func uniqueKeyMevSendBundle(b *MevSendBundleArgs, hash hash.Hash) {
	hash.Write([]byte(b.ReplacementUUID))
	_ = binary.Write(hash, binary.LittleEndian, b.Inclusion.BlockNumber)
	_ = binary.Write(hash, binary.LittleEndian, b.Inclusion.MaxBlock)
	for _, body := range b.Body {
		if body.Bundle != nil {
			uniqueKeyMevSendBundle(body.Bundle, hash)
		} else if body.Tx != nil {
			hash.Write(*body.Tx)
		}
		// body.Hash should not occur at this point
		if body.CanRevert {
			hash.Write([]byte{1})
		} else {
			hash.Write([]byte{0})
		}
		hash.Write([]byte(body.RevertMode))
	}
	_, _ = hash.Write(b.Metadata.Signer.Bytes())
}

func (b *EthSendRawTransactionArgs) UniqueKey() uuid.UUID {
	hash := newHash()
	_, _ = hash.Write(*b)
	return uuidFromHash(hash)
}

func (b *EthCancelBundleArgs) UniqueKey() uuid.UUID {
	hash := newHash()
	_, _ = hash.Write([]byte(b.ReplacementUUID))
	_, _ = hash.Write(b.SigningAddress.Bytes())
	return uuidFromHash(hash)
}

func (b *BidSubsisideBlockArgs) UniqueKey() uuid.UUID {
	hash := newHash()
	_ = binary.Write(hash, binary.LittleEndian, uint64(*b))
	return uuidFromHash(hash)
}
