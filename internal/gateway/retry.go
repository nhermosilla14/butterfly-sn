package gateway

import (
	"log/slog"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// Kinds of gateway message to the client that the retry set supervises. They are also the names the logs show.
const (
	kindRegister = "REGISTER"
	kindPublish  = "PUBLISH"
)

// retryKey identifies a supervised message by kind and msg id. A REGISTER and a PUBLISH may share an id.
type retryKey struct {
	kind string
	id   uint16
}

// retryEntry is a gateway message to the client that still awaits its reply. msg is the exact message sent,
// attempts counts its retransmissions and due is when it is sent again.
type retryEntry struct {
	msg      mqttsn.Message
	attempts int
	due      time.Time
}

// sendSupervised sends m to the client and records it in the retry set until its reply arrives. kind and id are
// its type and msg id, and m must be a REGISTER or a QoS 1 PUBLISH. When m cannot be sent as one datagram it returns
// an error wrapping errTooLarge and records nothing, so the caller drops it.
func (s *session) sendSupervised(kind string, id uint16, m mqttsn.Message) error {
	if err := s.gw.send(s.addr, m); err != nil {
		return err
	}
	s.retries[retryKey{kind: kind, id: id}] = &retryEntry{msg: m, due: time.Now().Add(s.gw.retryInterval)}
	return nil
}

// ackRetry removes the entry of a reply that arrived, and any parked copy of it. An id with no entry is ignored.
func (s *session) ackRetry(kind string, id uint16) {
	k := retryKey{kind: kind, id: id}
	delete(s.retries, k)
	delete(s.parked, k)
}

// clearRetries drops every entry when the client goes to sleep, so a sleeping client is never retransmitted to. A
// QoS 1 PUBLISH still awaiting the client's PUBACK is parked instead, with its datagram, until the wake. A flush
// after the wake records fresh entries (see restoreParked).
func (s *session) clearRetries() {
	for k, e := range s.retries {
		if k.kind == kindPublish && s.downlinkAwaitingPuback(k.id) {
			s.parked[k] = e.msg
		}
	}
	clear(s.retries)
}

// restoreParked supervises again the parked QoS 1 PUBLISHes at the start of a wake-up flush. Each gets a fresh
// attempt count and due time, so it is retransmitted with DUP like any other entry.
func (s *session) restoreParked() {
	due := time.Now().Add(s.gw.retryInterval)
	for k, msg := range s.parked {
		s.retries[k] = &retryEntry{msg: msg, due: due}
	}
	clear(s.parked)
}

// armRetry returns the channel of the retry timer, armed for the earliest of the retry due times and the broker
// request deadlines. It is nil when neither is pending, so the run loop's case never fires. Each call replaces the
// previous timer.
func (s *session) armRetry() <-chan time.Time {
	s.stopRetryTimer()
	var earliest time.Time
	consider := func(t time.Time) {
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	for _, e := range s.retries {
		consider(e.due)
	}
	budget := s.requestBudget()
	for _, p := range s.pendingSubs {
		consider(p.sent.Add(budget))
	}
	for _, p := range s.pendingUnsubs {
		consider(p.sent.Add(budget))
	}
	if earliest.IsZero() {
		return nil
	}
	s.retryTimer = time.NewTimer(time.Until(earliest))
	return s.retryTimer.C
}

// requestBudget is how long the broker may leave a SUBSCRIBE or UNSUBSCRIBE unanswered: retry interval times retry
// limit, the same budget a client gets for its retransmissions.
func (s *session) requestBudget() time.Duration {
	return s.gw.retryInterval * time.Duration(s.gw.retryLimit)
}

// checkBrokerRequests returns errBrokerSilent when a pending SUBSCRIBE or UNSUBSCRIBE has used its whole budget.
// Requests are not retransmitted to the broker. The request is logged by type and msg id, never by topic.
func (s *session) checkBrokerRequests(log *slog.Logger) error {
	budget := s.requestBudget()
	now := time.Now()
	for id, p := range s.pendingSubs {
		if now.Sub(p.sent) >= budget {
			log.Warn("broker did not answer request in time", "type", "SUBSCRIBE", "msg_id", id)
			return errBrokerSilent
		}
	}
	for id, p := range s.pendingUnsubs {
		if now.Sub(p.sent) >= budget {
			log.Warn("broker did not answer request in time", "type", "UNSUBSCRIBE", "msg_id", id)
			return errBrokerSilent
		}
	}
	return nil
}

// stopRetryTimer stops the retry timer, if one is armed.
func (s *session) stopRetryTimer() {
	if s.retryTimer != nil {
		s.retryTimer.Stop()
		s.retryTimer = nil
	}
}

// retransmitDue resends every entry whose due time has passed. A PUBLISH is resent with DUP set, and every other
// message unchanged. Its due time moves one retry interval on. It returns true when an entry has used its
// retransmissions, so the client must be declared lost. Logs carry the client id, type and msg id, never payloads.
func (s *session) retransmitDue(log *slog.Logger) (lost bool) {
	now := time.Now()
	for k, e := range s.retries {
		if e.due.After(now) {
			continue
		}
		if e.attempts >= s.gw.retryLimit {
			log.Warn("client did not answer retransmissions, disconnecting", "type", k.kind, "msg_id", k.id, "attempts", e.attempts)
			return true
		}
		e.attempts++
		if p, ok := e.msg.(mqttsn.Publish); ok {
			p.DUP = true
			e.msg = p
		}
		s.gw.sendTo(s.addr, e.msg)
		e.due = now.Add(s.gw.retryInterval)
		log.Info("retransmitting to client", "type", k.kind, "msg_id", k.id, "attempt", e.attempts)
	}
	return false
}
