/*
 * Copyright 2021-present, StarRocks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package statefulset

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
)

// Status returns a message describing statefulset status, and a bool value indicating if the status is considered done.
// Copy from kubelet, extended with the OnDelete strategy: its pods are replaced by whoever deletes them, so the
// update is done once every pod runs the update revision.
func Status(sts *appsv1.StatefulSet) (string, bool, error) {
	switch sts.Spec.UpdateStrategy.Type {
	case appsv1.RollingUpdateStatefulSetStrategyType, appsv1.OnDeleteStatefulSetStrategyType:
	default:
		return "", true, fmt.Errorf("rollout status is only available for %s and %s strategy types",
			appsv1.RollingUpdateStatefulSetStrategyType, appsv1.OnDeleteStatefulSetStrategyType)
	}
	if sts.Status.ObservedGeneration == 0 || sts.Generation > sts.Status.ObservedGeneration {
		return "Waiting for statefulset spec update to be observed", false, nil
	}
	if sts.Spec.Replicas != nil && sts.Status.ReadyReplicas < *sts.Spec.Replicas {
		return fmt.Sprintf("Waiting for %d pods to be ready", *sts.Spec.Replicas-sts.Status.ReadyReplicas), false, nil
	}
	if sts.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		if !RevisionRolledOut(sts) {
			return fmt.Sprintf("waiting for %d pods to be replaced at revision %s",
				DesiredReplicas(sts)-sts.Status.UpdatedReplicas, sts.Status.UpdateRevision), false, nil
		}
		return fmt.Sprintf("statefulset OnDelete update complete %d pods at revision %s",
			sts.Status.UpdatedReplicas, sts.Status.UpdateRevision), true, nil
	}
	if sts.Spec.UpdateStrategy.Type == appsv1.RollingUpdateStatefulSetStrategyType && sts.Spec.UpdateStrategy.RollingUpdate != nil {
		if sts.Spec.Replicas != nil && sts.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
			if sts.Status.UpdatedReplicas < (*sts.Spec.Replicas - *sts.Spec.UpdateStrategy.RollingUpdate.Partition) {
				return fmt.Sprintf("Waiting for partitioned roll out to finish: %d out of %d new pods have been updated",
					sts.Status.UpdatedReplicas, *sts.Spec.Replicas-*sts.Spec.UpdateStrategy.RollingUpdate.Partition), false, nil
			}
		}
		return fmt.Sprintf("partitioned roll out complete: %d new pods have been updated", sts.Status.UpdatedReplicas), true, nil
	}
	if sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		return fmt.Sprintf("waiting for statefulset rolling update to complete %d pods at revision %s",
			sts.Status.UpdatedReplicas, sts.Status.UpdateRevision), false, nil
	}
	return fmt.Sprintf("statefulset rolling update complete %d pods at revision %s",
		sts.Status.CurrentReplicas, sts.Status.CurrentRevision), true, nil
}

// DesiredReplicas returns spec.replicas, or the API default of 1 when it is not set.
func DesiredReplicas(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas == nil {
		return 1
	}
	return *sts.Spec.Replicas
}

// RevisionRolledOut reports whether every pod of the statefulset runs the update revision.
// With the RollingUpdate strategy the statefulset controller records that by setting currentRevision to
// updateRevision. With OnDelete it never does, so the number of pods at the update revision is compared
// with the desired replicas instead.
func RevisionRolledOut(sts *appsv1.StatefulSet) bool {
	if sts.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		return sts.Status.UpdatedReplicas >= DesiredReplicas(sts)
	}
	return sts.Status.UpdateRevision == sts.Status.CurrentRevision
}
