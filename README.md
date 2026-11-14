# Pantopic Cluster
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fpantopic%2Fcluster.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fpantopic%2Fcluster?ref=badge_shield)


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


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fpantopic%2Fcluster.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fpantopic%2Fcluster?ref=badge_large)