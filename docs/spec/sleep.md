# Sleep

How a client sleeps without dropping its broker connection, how the gateway buffers downlink messages, and how a
wake flushes them.

## Behaviours

### Sleep entry and expiry
DISCONNECT with a duration above 0 puts the session to sleep. The broker connection stays open, the active
inactivity timer is replaced by a sleep timer of 1.5 times the duration, and broker keepalive continues. A client
asleep past that limit is declared lost and torn down with the will-aware teardown. DISCONNECT without a duration
from asleep is a normal disconnect. DISCONNECT with a duration from a sleeping or awake client is a re-sleep: it
changes the sleep duration and the client stays asleep (MQTT-SN 6.14).
- Criteria:
  - Sleep entry replies DISCONNECT and keeps the broker connection open.
  - A sleeping client is not timed out by its CONNECT keepalive, and keepalive pings still go to the broker.
  - Sleep expiry sends the client DISCONNECT, frees the slot and closes the broker connection; a will is published
    when one exists and MQTT DISCONNECT is sent when it does not.
  - DISCONNECT without a duration, or with duration 0, from asleep ends the session gracefully with no will.
  - DISCONNECT with a duration from asleep replaces the sleep duration, restarts the sleep timer at 1.5 times the
    new duration and gets DISCONNECT; session, broker connection, subscriptions and buffered messages are kept
    and no will is published. Expiry then follows the new duration.
  - The same DISCONNECT during a wake-up flush (PINGREQ or CONNECT wake) interrupts it: messages not yet sent,
    and those held for a pending REGISTER, stay buffered in order without a second broker PUBACK (the buffer
    still holds at most 20, newest dropped), pending retries are dropped, a late PUBACK of an already sent
    message is still absorbed, and no PINGRESP is sent for the interrupted flush.
  - Shutdown and broker loss while asleep keep their existing behaviour.
- Tests: TestSleepEntryKeepsBrokerConnectionAndPings, TestSleepDoesNotTimeOutAtInactivityLimit, TestSleepExpiryWithWillClosesBrokerBare, TestSleepExpiryWithoutWillSendsDisconnect, TestDisconnectWhileAsleepIsNormal, TestResleepShorterDurationExpiresOnNewSchedule, TestResleepWhileAsleepKeepsBufferedForNextPingreq, TestResleepMidFlushKeepsOrderWithoutSecondPuback, TestResleepAbsorbsLatePubackOfFlushedQoS1, TestResleepDurationZeroEndsSession, TestResleepDuringConnectWakeFlushReturnsToSleep, TestResleepCapDropsNewestWhenHeldReturns, TestShutdownWhileAsleepSendsDisconnect, TestSleepExpiryPublishesWillAndFreesSlot, TestSleepKeepsSessionAndDisconnectFreesSlot

### Buffering
Broker messages for a sleeping client are buffered instead of delivered. QoS 1 is acknowledged to the broker when
buffered; QoS 0 needs no ack. The buffer holds 20 messages and drops the newest on overflow. Messages held for an
unanswered gateway REGISTER move into the buffer when the client sleeps.
- Criteria:
  - No buffered message reaches the client while it sleeps.
  - QoS 1 gets an immediate broker PUBACK; QoS 0 gets none.
  - The 21st message is dropped and logged.
  - A PUBLISH held for a pending REGISTER is buffered, acknowledged and delivered after a fresh REGISTER on wake.
- Tests: TestSleepBuffersDownlinkWithoutDelivery, TestSleepBufferOverflowDropsNewestAndAcksQoS1, TestSleepingSubscriberGetsNoDelivery, TestSleepMovesHeldRegisterToBufferAndWakeRegistersAgain

### Wake-up flush
PINGREQ from asleep wakes the client and flushes the buffer in order; PINGRESP is the last datagram and the client
sleeps again. CONNECT from asleep gets CONNACK, flushes and stays active. Unknown topics are registered before
their PUBLISH. Buffered QoS 1 messages use gateway-assigned msg ids and are not acknowledged to the broker twice.
- Criteria:
  - An empty-buffer PINGREQ gets PINGRESP and the client stays asleep with the timer restarted.
  - Buffered messages arrive in order and PINGRESP is last, then the client is asleep again.
  - An unknown topic gets REGISTER first; a rejected REGACK drops the message and the flush still finishes with
    PINGRESP.
  - A PUBLISH arriving during the flush is delivered after the already buffered ones and before PINGRESP.
  - CONNECT from asleep gets CONNACK without a new broker connection, flushes, stays active and re-arms the
    original keepalive timer; live downlink works afterwards.
  - Buffered QoS 1 messages get nonzero gateway msg ids; client PUBACKs produce no second broker PUBACK, even
    after the flush; the pre-acknowledged set is capped at 20.
  - An active QoS 1 message whose broker packet id collides with a pre-acknowledged id is dropped and acknowledged
    to the broker instead of duplicating a client msg id.
  - A REGISTER or PUBLISH of the flush follows the retry rule of [topics-and-pubsub.md](topics-and-pubsub.md): a
    missing REGACK or PUBACK ends in DISCONNECT instead of waiting, and a sleeping client is never retransmitted
    to. A downlink message too large for one UDP datagram is dropped on flush, without ending the flush
    (see [topics-and-pubsub.md](topics-and-pubsub.md)). A QoS 1 PUBLISH still unacknowledged when the client sleeps is not resent while it sleeps and is
    supervised again, with fresh retries, from the wake-up.
- Tests: TestOversizeBufferedMessageDroppedOnFlushThenPingresp, TestRetryFlushMissingRegackEndsWithDisconnect, TestRetrySleepingClientNotRetransmittedOrTimedOut, TestRetrySleepParkedQoS1ResentWithDupAfterWake, TestRetrySleepParkedQoS1SilentAfterWakeDisconnects, TestWakeEmptyPingreqGetsPingrespAndStaysAsleep, TestWakeFlushesQoS0InOrderThenPingresp, TestWakeUnknownTopicRegistersBeforePingresp, TestWakeRejectedRegackDropsBufferedAndFinishes, TestWakeDuringFlushDeliveredAfterBufferedBeforePingresp, TestWakeConnectFlushesAndStaysActive, TestWakeConnectRearmsInactivityWithOriginalDuration, TestWakeRestartsSleepTimerAfterPingreq, TestWakeQoS1GetsGatewayMsgIDAndNoSecondBrokerPuback, TestWakeDuplicateBrokerIDGetsDistinctMsgIDs, TestWakePreAckedCapDropsFurtherQoS1, TestActiveQoS1WithPreAckedIDIsDropped, TestWakePingreqFlushesInOrderThenPingresp, TestWakeWildcardUnregisteredTopicRegistersBeforePingresp, TestWakeConnectFlushesThenDeliversLive

## Not supported
- Retransmission of PINGREQ towards the client.
- Retry supervision of a flushed QoS 1 PUBLISH that the broker already acknowledged and the client has not
  acknowledged when it goes back to sleep: it is dropped from supervision.
- Persistent sessions (CleanSession=0), will persistence and session resumption across gateways.
- Queue ageing, byte-size limits, multiple sleep durations and will updates.
