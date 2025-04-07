package proxy

import (
	"fmt"

	"github.com/VictoriaMetrics/metrics"
)

var (
	archiveEventsProcessedTotalCounter = metrics.NewCounter("orderflow_proxy_archive_events_processed_total")
	archiveEventsProcessedErrCounter   = metrics.NewCounter("orderflow_proxy_archive_events_processed_err")
	// number of events sent successfully to orderflow archive. i.e. if we batch 10 events into 1 request this would be increment by 10
	archiveEventsRPCSentCounter = metrics.NewCounter("orderflow_proxy_archive_events_sent_ok")
	archiveEventsRPCDuration    = metrics.NewSummary("orderflow_proxy_archive_rpc_duration_milliseconds")
	archiveEventsRPCErrors      = metrics.NewCounter("orderflow_proxy_archive_rpc_errors")

	confighubErrorsCounter = metrics.NewCounter("orderflow_proxy_confighub_errors")

	shareQueueInternalErrors = metrics.NewCounter("orderflow_proxy_share_queue_internal_errors")

	apiUserRateLimits = metrics.NewCounter("orderflow_proxy_api_user_rate_limits")
)

const (
	apiIncomingRequestsByPeer  = `orderflow_proxy_api_incoming_requests_by_peer{peer="%s"}`
	apiDuplicateRequestsByPeer = `orderflow_proxy_api_duplicate_requests_by_peer{peer="%s"}`

	shareQueuePeerStallingErrorsLabel = `orderflow_proxy_share_queue_peer_stalling_errors{peer="%s"}`
	shareQueuePeerRPCErrorsLabel      = `orderflow_proxy_share_queue_peer_rpc_errors{peer="%s"}`
	shareQueuePeerRPCDurationLabel    = `orderflow_proxy_share_queue_peer_rpc_duration_milliseconds{peer="%s"}`
)

func incAPIIncomingRequestsByPeer(peer string) {
	l := fmt.Sprintf(apiIncomingRequestsByPeer, peer)
	metrics.GetOrCreateCounter(l).Inc()
}

func incAPIDuplicateRequestsByPeer(peer string) {
	l := fmt.Sprintf(apiDuplicateRequestsByPeer, peer)
	metrics.GetOrCreateCounter(l).Inc()
}

func incAPIUserRateLimits() {
	apiUserRateLimits.Inc()
}

func incShareQueuePeerStallingErrors(peer string) {
	l := fmt.Sprintf(shareQueuePeerStallingErrorsLabel, peer)
	metrics.GetOrCreateCounter(l).Inc()
}

func incShareQueuePeerRPCErrors(peer string) {
	l := fmt.Sprintf(shareQueuePeerRPCErrorsLabel, peer)
	metrics.GetOrCreateCounter(l).Inc()
}

func timeShareQueuePeerRPCDuration(peer string, duration int64) {
	l := fmt.Sprintf(shareQueuePeerRPCDurationLabel, peer)
	metrics.GetOrCreateSummary(l).Update(float64(duration))
}
