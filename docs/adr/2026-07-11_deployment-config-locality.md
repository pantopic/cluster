# ADR - Deployment Registry Locality

Should the deployment registry be managed as a single shard (or group of shards) with selective caching on non-member
hosts rather than replicating to all hosts in all clusters using raft read replicas (like zongzi's cluster registry)?

## Advantages

1. Reduced raft heartbeat compute overhead
2. Reduced load on shard leader (which might be significant in larger clusters)
3. We have to build a cross-cluster replication mechanism anyways, so exporting a change data stream is already a
requirement. That replication mechanism should be identical for replicating inter-cluster state to caches.
4. May present tiered caching opportunities to more efficiently spread control load, especially useful for spikes in 
joining and recovering nodes.
5. May be backported to zongzi if it works well.

## Disadvantages

1. Yet another data replication mechanism.
2. Are we just reimplementing etcd given that we're implementing gRPC event streams (ie. watches)?
3. Is what we're building actually better than Turbokube in any meaningful way?
4. Could we add a tiered cache to dragonboat to make read replicas efficient?
5. Technically we could do this later with a migration.

### Should we just use a Turbokube shard (group) for the deployment registry?

1. Instant dog food.
2. Gives us a reason to scale it.
3. Tiered cache could be valuable to users.
4. We'd still need to build our own watch cache.
5. Alternate watch cache could be ported to Kubernetes if we discover some optimizations.
6. Collect metrics about Kubernetes use of etcd and devise caching strategies.
7. We would have to push all the data validation into the service layer with CAS which is a bit of a PITA.
8. Though we're already doing CAS in the control api and controllers anyways so `¯\_(ツ)_/¯`.
9. Persistent seems inevitable. Deployment registry is required for global recovery.

## Conclusion

Continue as is. Re-evaluate if and when it issues arise.

## History

2026-07-11 - KBurns - Draft
