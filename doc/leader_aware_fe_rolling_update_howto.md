# Roll FE pods out leader-last

This document describes how to make the operator replace the FE pods of a `StarRocksCluster` in an order that
keeps the leader FE for last, so that an image upgrade or any other change of the FE pod template costs at most
one leader election.

## Why the default order costs several leader elections

The operator deploys FE as a StatefulSet with the `RollingUpdate` strategy. The StatefulSet controller replaces
the pods from the highest ordinal down to 0 and does not know which FE is the leader. When the leader runs on
`fe-2` it is the first pod to go, FE elects a new leader among `fe-1` and `fe-0`, and when the new leader is the
next pod in line it is restarted too. A cluster of 3 FEs can therefore go through 2 elections during one
rolling update, a cluster of 5 through 4. While an election runs, every statement that has to reach the leader
(DDL, load transactions, INSERT) fails.

The StarRocks upgrade guide asks for the opposite order: upgrade every follower FE first and the leader FE last.
`leaderAwareRollingUpdate` makes the operator follow that order.

## How it works

With `leaderAwareRollingUpdate: true` in `starRocksFeSpec`:

1. The FE StatefulSet uses the `OnDelete` update strategy, so a pod only restarts with the new pod template when
   the operator deletes it. `updateStrategy` of the FE spec is ignored while the field is true.
2. Whenever a pod does not run the update revision of the StatefulSet, the operator runs `SHOW FRONTENDS`
   through the FE service and picks the next pod to replace: followers and observers first, highest ordinal
   first, and the leader only when every other pod is already updated.
3. Before deleting the leader pod, the operator runs `ALTER SYSTEM TRANSFER LEADER TO "<host>:<edit_log_port>"`
   towards the updated follower with the highest ordinal. When the statement succeeds, the leader steps down in
   place and its pod restarts as a follower, so the update costs no election at all. When it fails, because the
   FE version does not have the statement or the cluster runs in shared-data mode, the leader is restarted and
   one election happens. The outcome is recorded as an event.
4. The next pod is deleted only when every FE pod is Ready and every FE reports `Alive = true` in
   `SHOW FRONTENDS`. A replaced FE is Ready before the leader's next heartbeat reaches it, so the operator waits
   for that heartbeat as well.

Progress is visible in the events of the `StarRocksCluster` and in `status.starRocksFeStatus.reason`:

```bash
kubectl -n starrocks get events --field-selector reason=LeaderAwareRollingUpdate
kubectl -n starrocks get starrockscluster kube-starrocks -o jsonpath='{.status.starRocksFeStatus.reason}'
```

## Enable it

Add the field to the FE spec of the `StarRocksCluster`:

```yaml
apiVersion: starrocks.com/v1
kind: StarRocksCluster
metadata:
  name: kube-starrocks
  namespace: starrocks
spec:
  starRocksFeSpec:
    image: starrocks/fe-ubuntu:3.5.4
    replicas: 3
    leaderAwareRollingUpdate: true
    feEnvVars:
      - name: MYSQL_PWD
        valueFrom:
          secretKeyRef:
            name: starrocks-root-password
            key: password
```

or, with the Helm chart:

```yaml
starrocks:
  starrocksFESpec:
    leaderAwareRollingUpdate: true
```

The operator connects to FE as `root`. When root has a password, provide it through the `MYSQL_PWD`
environment variable of `feEnvVars`, exactly as for CN scale-in; a plain value or a reference to a ConfigMap
or Secret key both work. The `--fe-ssl-mode` flag of the operator applies to this connection too.

When `SHOW FRONTENDS` cannot be run, for example because the password is wrong or a NetworkPolicy blocks the
operator, the update pauses instead of guessing an order: a warning event with reason `LeaderAwareRollingUpdate`
explains why, and the update continues as soon as the query succeeds. Set `leaderAwareRollingUpdate: false` to go
back to the StatefulSet's own rolling update at any time; pods that already run the update revision are not
restarted by the switch.

## Notes

- The operator needs permission to delete pods. The bundled ClusterRole and Role grant it; a custom
  least-permission setup has to add the `delete` verb on `pods`.
- With a single FE there is nothing to reorder: the only pod is the leader and restarting it interrupts the
  cluster as before.
- `ALTER SYSTEM TRANSFER LEADER` is only attempted when an updated follower is alive. Observers are never
  chosen as the target.
- `waitForFullRollout` keeps working: BE and CN wait until every FE pod runs the update revision.
