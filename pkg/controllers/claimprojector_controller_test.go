/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, resourceapi.AddToScheme(s))
	return s
}

func TestClaimProjectorReconcileLifecycle(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
		},
	}

	opaqueParams, err := json.Marshal(v1alpha1.OpaqueConfig{
		APIVersion: v1alpha1.APIVersion,
		CPUConfig: v1alpha1.CPUConfig{
			CPUSet:      "0-3",
			Relocatable: false,
			Alignment:   v1alpha1.AlignmentBestEffort,
		},
	})
	require.NoError(t, err)

	claim1 := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "claim-1",
			Namespace: "default",
			UID:       types.UID("claim-uid-1"),
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "req-1",
					},
				},
			},
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Request: "req-1",
							Driver:  "dra.cpu",
							Pool:    "node-1",
							Device:  "cache-0",
						},
					},
					Config: []resourceapi.DeviceAllocationConfiguration{
						{
							Source: resourceapi.AllocationConfigSourceClaim,
							DeviceConfiguration: resourceapi.DeviceConfiguration{
								Opaque: &resourceapi.OpaqueDeviceConfiguration{
									Driver: "dra.cpu",
									Parameters: runtime.RawExtension{
										Raw: opaqueParams,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node, claim1).
		Build()

	r := &ClaimProjectorReconciler{
		Client:          client,
		Scheme:          scheme,
		DriverNamespace: "default",
		DriverName:      "dra.cpu",
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	res, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	cmName := v1alpha1.ProjectedClaimsConfigMapName("node-1")
	var cm corev1.ConfigMap
	err = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: cmName}, &cm)
	require.NoError(t, err)

	var proj v1alpha1.ProjectedClaims
	err = json.Unmarshal([]byte(cm.Data[v1alpha1.ProjectedClaimsKey]), &proj)
	require.NoError(t, err)
	require.Equal(t, v1alpha1.APIVersion, proj.APIVersion)
	require.Equal(t, int64(1), proj.Generation)
	require.Len(t, proj.Claims, 1)

	pc := proj.Claims[0]
	require.Equal(t, "claim-uid-1", pc.UID)
	require.Equal(t, "claim-1", pc.Name)
	require.Equal(t, "default", pc.Namespace)
	require.Equal(t, v1alpha1.ClaimStateAllocated, pc.State)
	require.Equal(t, "never-split", pc.Shape)
	require.False(t, pc.OffersSplitAlternatives)
	require.Len(t, pc.Devices, 1)
	require.Equal(t, "req-1", pc.Devices[0].Request)
	require.Equal(t, "node-1", pc.Devices[0].Pool)
	require.Equal(t, "cache-0", pc.Devices[0].Device)
	require.Equal(t, v1alpha1.AlignmentBestEffort, pc.CPUConfig.Alignment)

	res, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	err = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: cmName}, &cm)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(cm.Data[v1alpha1.ProjectedClaimsKey]), &proj)
	require.NoError(t, err)
	require.Equal(t, int64(1), proj.Generation)

	claim1.Status.Allocation = nil
	require.NoError(t, client.Update(ctx, claim1))

	res, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	err = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: cmName}, &cm)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(cm.Data[v1alpha1.ProjectedClaimsKey]), &proj)
	require.NoError(t, err)
	require.Equal(t, int64(2), proj.Generation)
	require.Len(t, proj.Claims, 1)
	require.Equal(t, v1alpha1.ClaimStateDeallocated, proj.Claims[0].State)

	res, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	err = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: cmName}, &cm)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(cm.Data[v1alpha1.ProjectedClaimsKey]), &proj)
	require.NoError(t, err)
	require.Equal(t, int64(3), proj.Generation)
	require.Empty(t, proj.Claims)

	require.NoError(t, client.Delete(ctx, node))
	res, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	err = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: cmName}, &cm)
	require.True(t, apierrs.IsNotFound(err))
}

func TestClaimProjectorFlexibleClaimAndNodeSelector(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-2",
		},
	}

	opaqueParams, err := json.Marshal(v1alpha1.OpaqueConfig{
		APIVersion: v1alpha1.APIVersion,
		CPUConfig: v1alpha1.CPUConfig{
			Relocatable: true,
			Alignment:   v1alpha1.AlignmentRepairable,
		},
	})
	require.NoError(t, err)

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "flexible-claim",
			Namespace: "tenant-a",
			UID:       types.UID("flex-uid"),
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "req-flex",
						FirstAvailable: []resourceapi.DeviceSubRequest{
							{Name: "sub-1"},
							{Name: "sub-2"},
						},
					},
				},
			},
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				NodeSelector: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"node-2"},
								},
							},
						},
					},
				},
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Request: "req-flex/sub-1",
							Driver:  "dra.cpu",
							Pool:    "node-2",
							Device:  "cache-1",
						},
					},
					Config: []resourceapi.DeviceAllocationConfiguration{
						{
							Source: resourceapi.AllocationConfigSourceClaim,
							DeviceConfiguration: resourceapi.DeviceConfiguration{
								Opaque: &resourceapi.OpaqueDeviceConfiguration{
									Driver: "dra.cpu",
									Parameters: runtime.RawExtension{
										Raw: opaqueParams,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node, claim).
		Build()

	r := &ClaimProjectorReconciler{
		Client:          client,
		Scheme:          scheme,
		DriverNamespace: "driver-ns",
		DriverName:      "dra.cpu",
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "node-2"}}
	res, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	cmName := v1alpha1.ProjectedClaimsConfigMapName("node-2")
	var cm corev1.ConfigMap
	err = client.Get(ctx, types.NamespacedName{Namespace: "driver-ns", Name: cmName}, &cm)
	require.NoError(t, err)

	var proj v1alpha1.ProjectedClaims
	err = json.Unmarshal([]byte(cm.Data[v1alpha1.ProjectedClaimsKey]), &proj)
	require.NoError(t, err)
	require.Equal(t, int64(1), proj.Generation)
	require.Len(t, proj.Claims, 1)

	pc := proj.Claims[0]
	require.Equal(t, "flex-uid", pc.UID)
	require.Equal(t, "flexible-claim", pc.Name)
	require.Equal(t, "tenant-a", pc.Namespace)
	require.Equal(t, "flexible", pc.Shape)
	require.True(t, pc.OffersSplitAlternatives)
	require.True(t, pc.CPUConfig.Relocatable)
	require.Equal(t, v1alpha1.AlignmentRepairable, pc.CPUConfig.Alignment)
	require.Len(t, pc.Devices, 1)
	require.Equal(t, "req-flex/sub-1", pc.Devices[0].Request)
}

func TestClaimProjectorIgnoresOtherDrivers(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-3",
		},
	}

	otherClaim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gpu-claim",
			Namespace: "default",
			UID:       types.UID("gpu-uid"),
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Request: "req-gpu",
							Driver:  "gpu.example.com",
							Pool:    "node-3",
							Device:  "gpu-0",
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node, otherClaim).
		Build()

	r := &ClaimProjectorReconciler{
		Client:          client,
		Scheme:          scheme,
		DriverNamespace: "default",
		DriverName:      "dra.cpu",
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "node-3"}}
	res, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, res)

	cmName := v1alpha1.ProjectedClaimsConfigMapName("node-3")
	var cm corev1.ConfigMap
	err = client.Get(ctx, types.NamespacedName{Namespace: "default", Name: cmName}, &cm)
	require.NoError(t, err)

	var proj v1alpha1.ProjectedClaims
	err = json.Unmarshal([]byte(cm.Data[v1alpha1.ProjectedClaimsKey]), &proj)
	require.NoError(t, err)
	require.Empty(t, proj.Claims)
}
