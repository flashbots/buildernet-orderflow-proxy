package proxy

import (
	"errors"

	"github.com/flashbots/go-utils/rpctypes"
)

var (
	errSigningAddress = errors.New("signing address field should not be set")

	errDroppingTxHashed = errors.New("dropping tx hashes field should not be set")
	errUUID             = errors.New("uuid field should not be set")
	errRefundPercent    = errors.New("refund percent field should not be set")
	errRefundRecipient  = errors.New("refund recipient field should not be set")
	errRefundTxHashes   = errors.New("refund tx hashes field should not be set")

	errLocalEndpointSbundleMetadata = errors.New("mev share bundle should not containt metadata when sent to local endpoint")
)

func ValidateEthSendBundle(args *rpctypes.EthSendBundleArgs, publicEndpoint bool) error {
	if !publicEndpoint {
		if args.SigningAddress != nil {
			return errSigningAddress
		}
	}

	// do not allow for new bundle fields for public endpoint
	if publicEndpoint {
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

	if args.UUID != nil {
		return errUUID
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
