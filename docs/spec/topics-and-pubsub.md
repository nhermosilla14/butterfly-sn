# Topics and pub/sub

How MQTT-SN clients map topic names to numeric ids, publish to the broker, subscribe with normal, wildcard or
short names, and receive broker messages in both directions.

## Behaviours

### Register topic
REGISTER adds a normal topic name to the client's own table and answers REGACK. Ids are allocated from 1 to
0xFFFE; registering the same name twice in one session returns the same id. The table holds at most 50 names. A
full table is answered with REGACK Congestion and never reuses an id. Names that are empty or contain `+` or `#`
are refused with REGACK Invalid topic id and are not stored.
- Criteria:
  - REGISTER gets REGACK Accepted with an id in 1–0xFFFE.
  - The same name registered twice returns the same id.
  - When the table is full, REGACK carries Congestion and the table does not wrap.
  - A wildcard or empty name gets REGACK Invalid topic id and leaves the table unchanged.
  - Ids stay unique and nonzero after a name is removed.
- Tests: TestRegisterReturnsStableTopicID, TestTopicTableIDs, TestTopicTableFullNeverWraps, TestRegisterWildcardRefusedAndNotTabled, TestTopicTableNamesUniqueAndNonzero, TestTopicTableRemoveThenAllocateIsUnique

### Publish QoS 0 from a client
A QoS 0 PUBLISH with a registered normal topic id, or with a short topic name, becomes an MQTT QoS 0 PUBLISH with
that topic name and an unchanged payload. An unknown id or an invalid name is dropped without a broker write or
client reply.
- Criteria:
  - The broker receives the registered topic name and the exact payload, binary included.
  - A PUBLISH with an unknown topic id reaches no broker topic and gets no acknowledgement.
  - A PUBLISH whose resolved name is empty or contains a wildcard is dropped and the session stays alive.
- Tests: TestPublishQoS0ReachesBroker, TestPublishQoS0UnknownTopicIDDropped, TestPublishWildcardTableNameDroppedWithoutBrokerWrite

### Publish QoS 1 from a client
A QoS 1 PUBLISH with a registered normal topic id, or with a short topic name, becomes an MQTT QoS 1 PUBLISH on
that client's connection, reusing the client's 16-bit msg id as the MQTT packet id. The client gets PUBACK
Accepted with its original msg id and topic id, and only after the broker acknowledged the publish.
- Criteria:
  - The broker receives the exact topic and payload at QoS 1.
  - No PUBACK is sent before the broker PUBACK arrives; the client PUBACK then carries the original msg id,
    topic id and Accepted.
  - Broker acknowledgements with an unknown packet id are ignored.
- Tests: TestPublishQoS1AcknowledgedAfterBrokerAcceptance, TestQoS1PubackWaitsForBrokerPuback, TestShortPublishReachesBrokerOnTwoCharName

### Subscribe and unsubscribe
SUBSCRIBE and UNSUBSCRIBE are forwarded on the client's own broker connection with the client's msg id as the MQTT
packet id. SUBACK reports the granted QoS; the topic id is the table id for an exact name, 0 for a wildcard filter
and the two name bytes for a short subscription. UNSUBACK copies the msg id. The client's table entry survives
UNSUBSCRIBE because its id stays valid for publishing.
- Criteria:
  - An exact SUBSCRIBE gets SUBACK Accepted with the granted QoS and a nonzero table id; the broker holds it.
  - A wildcard SUBSCRIBE gets SUBACK Accepted with topic id 0.
  - A short SUBSCRIBE gets SUBACK Accepted with the two name bytes as topic id and does not use a table slot.
  - A broker refusal maps to SUBACK Invalid topic id with QoS 0 and topic id 0.
  - QoS 2 and predefined topic ids get SUBACK Not supported without broker contact.
  - UNSUBSCRIBE gets UNSUBACK with the client's msg id; unknown broker acknowledgements are ignored.
  - Pending subscriptions and unsubscriptions are bounded at 20; overflow answers Congestion where the protocol
    has a return code.
- Tests: TestSubscribeExactReturnsAcceptedAndTopicID, TestSubscribeExactForwardsRegistersAndReportsTopicID, TestSubscribeWildcardReturnsTopicIDZero, TestSubscribeWildcardReportsZeroAndRegistersNothing, TestShortSubscribeReportsTwoBytesWithoutTableSlot, TestSubscribeBrokerRefusalMapsToInvalidTopicIDWithQoS0, TestSubscribeUnsupportedRefusedWithoutBrokerContact, TestSubscribePendingOverflowIsCongestion, TestUnsubscribeReturnsUnsubackWithMsgID, TestUnsubscribeForwardsAndKeepsTopicEntry, TestUnsubscribeUnsupportedAcknowledgedWithoutBrokerContact, TestUnknownSubackAndUnsubackAreIgnored

### Downlink publish
A broker PUBLISH for a topic in the client's table becomes one MQTT-SN PUBLISH with that id. An unknown topic gets
a gateway REGISTER and the PUBLISH is held until REGACK. Downlink QoS 1 uses the broker packet id as msg id and
acknowledges the broker only after the client PUBACK. For a sleeping client the message is buffered and QoS 1 is
acknowledged at buffering time instead (see [sleep.md](sleep.md)).
- Criteria:
  - A known topic arrives with its table id and payload, with no REGISTER.
  - An unknown topic arrives only after REGISTER; a second message for the same topic joins the same hold.
  - A rejected REGACK drops the held messages, acknowledges QoS 1 to the broker and forgets the name, so a later
    publish registers it again with a fresh id.
  - Downlink QoS 1 reaches the client with the broker packet id; the broker PUBACK follows the client PUBACK. A
    rejected client PUBACK is logged but still acknowledged to the broker.
  - A full table drops the message and acknowledges QoS 1 to the broker; held messages and pending QoS 1
    deliveries are bounded at 20 each.
  - A duplicate in-flight QoS 1 packet id is dropped without acknowledging the broker, and unknown REGACK or
    client PUBACK ids are ignored.
- Tests: TestDownlinkSubscribedTopicDeliveredWithoutRegister, TestDownlinkKnownTopicForwardedWithoutRegister, TestDownlinkWildcardUnregisteredTopicRegistersFirst, TestDownlinkUnknownTopicRegistersThenReleasesOnAccept, TestDownlinkQoS1AcknowledgedByClientAndFlowContinues, TestDownlinkQoS1BrokerPubackAfterClientPuback, TestDownlinkRegackRejectedDropsAndAcksQoS1, TestDownlinkRejectedRegistrationForgetsNameAndRegistersAgain, TestDownlinkSecondPublishJoinsPendingRegistration, TestDownlinkHoldFullDropsAndAcksQoS1, TestDownlinkTableFullDropsAndAcksQoS1, TestDownlinkFullTableDropsNewTopicWithoutRegister, TestDownlinkDuplicateQoS1IDDroppedWithoutSecondAck, TestDownlinkPendingFullAcksBrokerWithoutSending, TestDownlinkUnknownRegackAndPubackIgnored, TestDownlinkBrokerAckWriteFailureIsError

### Short topic names
A short topic name is exactly two characters, carried in the topic id field. It works in PUBLISH, SUBSCRIBE and
UNSUBSCRIBE, and a downlink message for an exact short subscription is sent as a short PUBLISH without REGISTER.
Short subscriptions live in a separate set capped at 50 names.
- Criteria:
  - A short PUBLISH with a valid two-character name reaches the broker; a zero byte or wildcard byte is dropped
    without a broker write and the session survives.
  - A short SUBSCRIBE reports the two name bytes and does not consume a table slot; a name of a different length
    gets Congestion, and the 51st short subscription gets Congestion.
  - A downlink message for a tracked short subscription arrives as a short PUBLISH with no REGISTER.
  - A one-character broker topic uses normal REGISTER, never the short form; a two-character topic subscribed
    normally uses its table id.
- Tests: TestShortPublishReachesBrokerLikeNormalPublish, TestShortPublishReachesBrokerOnTwoCharName, TestShortPublishWithZeroByteDroppedWithoutBrokerContact, TestShortPublishWithWildcardNameDroppedThenValidWorks, TestShortSubscribeReportsTwoBytesWithoutTableSlot, TestShortSubscribeDeliversShortPublishWithoutRegister, TestShortSubscribeWrongLengthGetsCongestion, TestShortSubscribeCapCongestionWithoutBrokerContact, TestShortUnsubscribeStopsShortDownlink, TestShortUnsubscribeInvalidLengthAcknowledgedWithoutBrokerContact, TestDownlinkShortSubscriptionSendsShortPublishWithoutRegister, TestDownlinkOneCharTopicWithWildcardRegisters, TestDownlinkOneCharTopicOnWildcardRegistersFirst, TestDownlinkTwoCharNormalSubscriptionUsesTableID, TestShortSubscribeBrokerRefusalNotTracked, TestTrackedShortNamesCountsTrackedAndPending, TestIsPublishableName

## Not supported
- Retransmission of REGISTER and PUBLISH towards the client, and DUP handling; later intent.
- Predefined topic ids (project non-goal).
- QoS 2 and broker TLS or authentication.
- Gateway-side message deduplication: a broker retransmission may be delivered again.
