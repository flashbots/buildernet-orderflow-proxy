package proxy

import (
	"errors"

	"github.com/flashbots/go-utils/rpctypes"
)

var (
	errSigningAddress = errors.New("signing address field should not be set")

	errDroppingTxHashed             = errors.New("dropping tx hashes field should not be set")
	errRefundPercent                = errors.New("refund percent field should not be set")
	errRefundRecipient              = errors.New("refund recipient field should not be set")
	errRefundTxHashes               = errors.New("refund tx hashes field should not be set")
	errReplacementSet               = errors.New("one of uuid or replacementUuid fields should be empty")
	errLocalEndpointSbundleMetadata = errors.New("mev share bundle should not contain metadata when sent to local endpoint")
	errVersionNotSet                = errors.New("version field should be set")
	errInvalidVersion               = errors.New("invalid version")
	errMoreThanOneRefundTxHash      = errors.New("no more than one refund tx hash is allowed")
)

// EnsureReplacementUUID updates bundle with consistent replacement uuid value (falling back to `uuid` field if needed)
func EnsureReplacementUUID(args *rpctypes.EthSendBundleArgs) (*rpctypes.EthSendBundleArgs, error) {
	if args.UUID == nil {
		return args, nil
	}
	if args.ReplacementUUID != nil {
		return nil, errReplacementSet
	}

	args.ReplacementUUID = args.UUID
	args.UUID = nil
	return args, nil
}

func IsVersionValid(version *string) bool {
	return version == nil || *version == "" || *version == rpctypes.BundleVersionV1 || *version == rpctypes.BundleVersionV2
}

func ValidateEthSendBundle(args *rpctypes.EthSendBundleArgs, publicEndpoint bool) error {
	if !publicEndpoint {
		if args.SigningAddress != nil {
			return errSigningAddress
		}
	}

	valid := IsVersionValid(args.Version)
	if !valid {
		return errInvalidVersion
	}

	if len(args.RefundTxHashes) > 1 {
		return errMoreThanOneRefundTxHash
	}

	if publicEndpoint {
		// For now public (system) endpoint should receive version field preset.
		// For flashbots endpoint orderflowproxy-sender uses it, for direct orderflow
		// first orderflow-proxy receiver sets this field
		// We might later change it once we successfully finish transition to v2 bundle support
		if args.Version == nil || *args.Version == "" {
			return errVersionNotSet
		}
		version := *args.Version
		if version == rpctypes.BundleVersionV2 {
			return nil
		}

		// check that v1 bundle dosn't have unsupported fields
		if len(args.DroppingTxHashes) > 0 {
			return errDroppingTxHashed
		}

		if args.RefundPercent != nil {
			return errRefundPercent
		}
		if args.RefundRecipient != nil {
			return errRefundRecipient
		}
		if len(args.RefundTxHashes) > 0 {
			return errRefundTxHashes
		}
	}

	return nil
}

func ValidateEthCancelBundle(args *rpctypes.EthCancelBundleArgs, publicEndpoint bool) error {
	if !publicEndpoint {
		if args.SigningAddress != nil {
			return errSigningAddress
		}
	}
	return nil
}

func ValidateMevSendBundle(args *rpctypes.MevSendBundleArgs, publicEndpoint bool) error {
	// @perf it calculates hash
	_, err := args.Validate()
	if err != nil {
		return err
	}

	if !publicEndpoint {
		if args.Metadata != nil {
			return errLocalEndpointSbundleMetadata
		}
	}

	return nil
}
