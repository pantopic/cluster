# Pantopic Cluster

- Cluster Registry (zongzi)
    - Host
    - Shard
    - Replica

- Global Registry
    - Namespace
    - Cluster
    - Resource
    - Component
    - Service
    # - Relay

## Services, Components and Relays

- Services are stateless
- Services can bind to ports to accept incoming connections
- Services cannot initiate outbound connections
- Components are stateful
- Components cannot accept incoming connections
- Components cannot initiate outbound connections
- Relays are stateless
- Relays cannot accept incoming connections
- Relays can initiate outbound connections
