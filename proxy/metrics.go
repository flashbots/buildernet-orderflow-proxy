package proxy

import (
	"fmt"
	"time"

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
	shareQueuePeerRPCDurationLabel    = `orderflow_proxy_share_queue_peer_rpc_duration_milliseconds{peer="%s",is_big="%t"}`
	shareQueuePeerE2EDurationLabel    = `orderflow_proxy_share_queue_peer_e2e_duration_milliseconds{peer="%s",method="%s",system_endpoint="%t",is_big="%t"}`
	shareQueuePeerQueueDurationLabel  = `orderflow_proxy_share_queue_peer_queue_duration_milliseconds{peer="%s",method="%s",system_endpoint="%t",is_big="%t"}`

	requestDurationLabel = `orderflow_proxy_api_request_processing_duration_milliseconds{method="%s",server_name="%s",step="%s"}`
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

func timeShareQueuePeerRPCDuration(peer string, duration int64, bigRequest bool) {
	l := fmt.Sprintf(shareQueuePeerRPCDurationLabel, peer, bigRequest)
	metrics.GetOrCreateSummary(l).Update(float64(duration))
}

func timeShareQueuePeerE2EDuration(peer string, duration time.Duration, method string, systemEndpoint, bigRequest bool) {
	millis := float64(duration.Microseconds()) / 1000.0
	l := fmt.Sprintf(shareQueuePeerE2EDurationLabel, peer, method, systemEndpoint, bigRequest)
	metrics.GetOrCreateSummary(l).Update(float64(millis))
}

func timeShareQueuePeerQueueDuration(peer string, duration time.Duration, method string, systemEndpoint, bigRequest bool) {
	millis := float64(duration.Microseconds()) / 1000.0
	l := fmt.Sprintf(shareQueuePeerQueueDurationLabel, peer, method, systemEndpoint, bigRequest)
	metrics.GetOrCreateSummary(l).Update(float64(millis))
}

func incRequestDurationStep(duration time.Duration, method, serverName, step string) {
	millis := float64(duration.Microseconds()) / 1000.0
	l := fmt.Sprintf(requestDurationLabel, method, serverName, step)
	metrics.GetOrCreateSummary(l).Update(millis)
}
