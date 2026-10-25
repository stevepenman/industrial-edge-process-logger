# Network Deployment

Do not run these changes on a development workstation. Commission `eth0` with a static DEMO-01
address, no default route, and no DNS. Configure `wlan0` as the only default-route and DNS
interface. Apply [the sysctl template](../sysctl/99-edge-network-isolation.conf) and
[the nftables policy](../nftables/edge-logger.nft), then verify `sysctl net.ipv4.ip_forward`,
`sysctl net.ipv6.conf.all.forwarding`, `ip route`, `nft list ruleset`, and `bridge link`.

The exact DEMO-01 static IP, subnet and allowed OPC UA port must be supplied during commissioning.
