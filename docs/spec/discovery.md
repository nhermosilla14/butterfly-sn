# Discovery

How clients find the gateway: they search with SEARCHGW and get a unicast GWINFO, or they listen for the
gateway's periodic ADVERTISE. Discovery never touches client sessions or the broker.

## Behaviours

### SEARCHGW and GWINFO
SEARCHGW from any sender, with or without a radius byte, is answered with one GWINFO unicast to that sender after
a random delay uniformly in 0 to 500 ms. The GWINFO carries the configured gateway id. The radius is ignored.
GWINFO and ADVERTISE received from clients or other gateways are logged and ignored.
- Criteria:
  - SEARCHGW with and without a radius gets exactly one GWINFO with the configured gateway id (1 to 255, default
    1) and no further datagram.
  - A SEARCHGW creates no session and does not contact the broker; a following non-CONNECT packet from the same
    sender still gets DISCONNECT as an unknown sender.
  - GWINFO and ADVERTISE from a sender get no reply and create no session.
  - Malformed discovery packets are ignored.
  - The delay is always within 0 to 500 ms and varies, and a reply with the production delay arrives in time.
  - A SEARCHGW received just before shutdown never causes a send after the socket closes, and Run returns nil.
- Tests: TestSearchgwGetsOneGwinfoWithoutSession, TestSearchgwGetsGwinfo, TestGwinfoAndAdvertiseFromUnknownSenderIgnored, TestGwinfoAndAdvertiseFromClientIgnored, TestMalformedDiscoveryPacketsIgnored, TestProductionDiscoveryDelayBounds, TestSearchgwReplyWithProductionDelayArrivesInTime, TestSearchgwThenShutdownRace

### ADVERTISE
The gateway advertises itself periodically with its own interval, independent of any client keepalive. Each
ADVERTISE carries the gateway id and a duration equal to the interval in seconds. The first one is sent after one
full interval, not at startup, and advertisement stops with the gateway. The destination is configurable and
defaults to the IPv4 broadcast address on the gateway's listen port.
- Criteria:
  - ADVERTISE arrives at the configured destination with the gateway id and the interval as its duration, and
    repeats on that interval; the first one does not arrive before one full interval.
  - An interval of 0 disables advertisement.
  - Shutdown stops the ticker: Run returns nil and nothing is sent afterwards.
  - With no destination configured, advertisement targets the broadcast address on the bound listen port; a
    failed send is logged at warn and the ticker keeps running.
  - An interval outside 0 to 65535 or an invalid advertise address makes startup fail with the key named.
- Tests: TestAdvertiseArrivesPeriodicallyAfterOneInterval, TestAdvertisePeriodic, TestAdvertiseDisabledSendsNothing, TestAdvertiseDisabled, TestAdvertiseShutdownStopsTicker, TestAdvertiseDefaultAddressUsesBoundPort, TestStartupInvalidAdvertise

## Not supported
- Multicast group membership: replies are unicast and advertisement goes to one configured destination.
- Suppressing a pending GWINFO when another gateway answers first.
- Rate limiting of SEARCHGW floods; the bounded delay is the only control.
- IPv6 advertisement destinations; discovery is UDP over IPv4.
- Containerised deployments need host networking for broadcast discovery to reach the local segment.
