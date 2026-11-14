# Topics and pub/sub

How MQTT-SN clients map topic names to numeric topic ids and publish to the broker. Client-to-broker direction
only; broker-to-client delivery is not implemented yet.

## Behaviours

### Register topic
REGISTER adds a normal topic name to the client's own table and answers REGACK. Ids are allocated from 1 to
0xFFFE; registering the same name twice in one session returns the same id. A full table is answered with
REGACK Congestion and never reuses an id.
- Criteria:
  - REGISTER gets REGACK Accepted with an id in 1–0xFFFE.
  - The same name registered twice returns the same id.
  - When the table is full, REGACK carries Congestion and the table does not wrap.
- Tests: TestRegisterReturnsStableTopicID, TestTopicTableIDs, TestTopicTableFullNeverWraps

### Publish QoS 0 from a client
A QoS 0 PUBLISH with a registered normal topic id becomes an MQTT QoS 0 PUBLISH with the registered topic name
and an unchanged payload. An unknown topic id is dropped without a broker write or client reply.
- Criteria:
  - The broker receives the registered topic name and the exact payload, binary included.
  - A PUBLISH with an unknown topic id reaches no broker topic and gets no acknowledgement.
- Tests: TestPublishQoS0ReachesBroker, TestPublishQoS0UnknownTopicIDDropped

### Publish QoS 1 from a client
A QoS 1 PUBLISH with a registered normal topic id becomes an MQTT QoS 1 PUBLISH on that client's connection,
reusing the client's 16-bit msg id as the MQTT packet id. The client gets PUBACK Accepted with its original msg
id and topic id, and only after the broker acknowledged the publish.
- Criteria:
  - The broker receives the exact topic and payload at QoS 1.
  - No PUBACK is sent before the broker PUBACK arrives; the client PUBACK then carries the original msg id,
    topic id and Accepted.
  - Broker acknowledgements with an unknown packet id are ignored.
- Tests: TestPublishQoS1AcknowledgedAfterBrokerAcceptance, TestQoS1PubackWaitsForBrokerPuback

## Not supported
- Client subscriptions and broker-to-client PUBLISH; planned. The MQTT client has observer-only QoS 0
  SUBSCRIBE/SUBACK and PUBLISH reads for the test suite.
- Wildcard topic filters and short or predefined topic ids; planned.
- PUBACK with an invalid-topic-id return code for an unknown topic: a QoS 1 PUBLISH with an unknown topic id,
  msg id 0 or a msg id still awaiting its broker PUBACK is dropped and logged without a reply; later intent.
- Retransmission of unacknowledged PUBLISH and the DUP flow; planned.
- QoS 2; planned for a later release.
