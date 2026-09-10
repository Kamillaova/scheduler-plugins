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
	"fmt"
	"reflect"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
)

// ClaimProjectorReconciler projects allocated ResourceClaims into per-node ConfigMaps.
type ClaimProjectorReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	DriverNamespace string
	DriverName      string
	Workers         int
}

func (r *ClaimProjectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.DriverNamespace == "" {
		r.DriverNamespace = metav1.NamespaceDefault
	}
	if r.DriverName == "" {
		r.DriverName = "dra.cpu"
	}
	workers := r.Workers
	if workers <= 0 {
		workers = 1
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Watches(&resourceapi.ResourceClaim{}, handler.EnqueueRequestsFromMapFunc(r.claimToNodeRequests)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.configMapToNodeRequests)).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers}).
		Complete(r)
}

func (r *ClaimProjectorReconciler) claimToNodeRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil
	}
	nodes := r.extractNodesForClaim(claim)
	requests := make([]reconcile.Request, 0, len(nodes))
	for _, node := range nodes {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: node}})
	}
	return requests
}

func (r *ClaimProjectorReconciler) configMapToNodeRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm.Namespace != r.DriverNamespace {
		return nil
	}
	nodeName, ok := strings.CutPrefix(cm.Name, "dra-cpu-claims-")
	if !ok || len(nodeName) == 0 {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: nodeName}}}
}

func (r *ClaimProjectorReconciler) extractNodesForClaim(claim *resourceapi.ResourceClaim) []string {
	if claim == nil || claim.Status.Allocation == nil {
		return nil
	}
	nodeSet := make(map[string]struct{})
	for _, res := range claim.Status.Allocation.Devices.Results {
		if res.Driver == r.DriverName && res.Pool != "" {
			nodeSet[res.Pool] = struct{}{}
		}
	}
	if claim.Status.Allocation.NodeSelector != nil {
		for _, term := range claim.Status.Allocation.NodeSelector.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn {
					for _, v := range expr.Values {
						if v != "" {
							nodeSet[v] = struct{}{}
						}
					}
				}
			}
		}
	}
	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func isNodeSelectorMatch(sel *corev1.NodeSelector, nodeName string) bool {
	if sel == nil {
		return false
	}
	for _, term := range sel.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn {
				for _, v := range expr.Values {
					if v == nodeName {
						return true
					}
				}
			}
		}
	}
	return false
}

func (r *ClaimProjectorReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	nodeName := req.Name

	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		if apierrs.IsNotFound(err) {
			cmName := v1alpha1.ProjectedClaimsConfigMapName(nodeName)
			var cm corev1.ConfigMap
			if getErr := r.Get(ctx, types.NamespacedName{Namespace: r.DriverNamespace, Name: cmName}, &cm); getErr == nil {
				if delErr := r.Delete(ctx, &cm); delErr != nil && !apierrs.IsNotFound(delErr) {
					return ctrl.Result{}, delErr
				}
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	cmName := v1alpha1.ProjectedClaimsConfigMapName(nodeName)
	var existingCM corev1.ConfigMap
	cmExists := true
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.DriverNamespace, Name: cmName}, &existingCM); err != nil {
		if apierrs.IsNotFound(err) {
			cmExists = false
		} else {
			return ctrl.Result{}, err
		}
	}

	var existingProjected v1alpha1.ProjectedClaims
	var lastGen int64
	if cmExists {
		if data, ok := existingCM.Data[v1alpha1.ProjectedClaimsKey]; ok && data != "" {
			if err := json.Unmarshal([]byte(data), &existingProjected); err == nil {
				lastGen = existingProjected.Generation
			}
		}
	}

	var claimList resourceapi.ResourceClaimList
	if err := r.List(ctx, &claimList); err != nil {
		return ctrl.Result{}, err
	}

	allocatedByUID := make(map[string]v1alpha1.ProjectedClaim)
	for i := range claimList.Items {
		claim := &claimList.Items[i]
		if claim.Status.Allocation == nil {
			continue
		}
		var devices []v1alpha1.ProjectedDevice
		for _, res := range claim.Status.Allocation.Devices.Results {
			if res.Driver == r.DriverName && (res.Pool == nodeName || (res.Pool == "" && isNodeSelectorMatch(claim.Status.Allocation.NodeSelector, nodeName))) {
				devices = append(devices, v1alpha1.ProjectedDevice{
					Request: res.Request,
					Pool:    res.Pool,
					Device:  res.Device,
				})
			}
		}
		if len(devices) == 0 {
			continue
		}
		sort.Slice(devices, func(i, j int) bool {
			if devices[i].Request != devices[j].Request {
				return devices[i].Request < devices[j].Request
			}
			return devices[i].Device < devices[j].Device
		})

		var cpuConfig v1alpha1.CPUConfig
		for _, cfg := range claim.Status.Allocation.Devices.Config {
			if cfg.Opaque != nil && cfg.Opaque.Driver == r.DriverName && len(cfg.Opaque.Parameters.Raw) > 0 {
				var opaque v1alpha1.OpaqueConfig
				if err := json.Unmarshal(cfg.Opaque.Parameters.Raw, &opaque); err == nil {
					cpuConfig = opaque.CPUConfig
					break
				}
			}
		}
		if cpuConfig.Alignment == "" {
			cpuConfig.Alignment = v1alpha1.AlignmentBestEffort
		}

		offersSplit := false
		for _, req := range claim.Spec.Devices.Requests {
			if len(req.FirstAvailable) > 1 {
				offersSplit = true
				break
			}
		}
		shape := api.ShapeNeverSplit
		if offersSplit {
			shape = api.ShapeFlexible
		}

		state := v1alpha1.ClaimStateAllocated
		if claim.DeletionTimestamp != nil {
			state = v1alpha1.ClaimStateDeallocated
		}

		uid := string(claim.UID)
		allocatedByUID[uid] = v1alpha1.ProjectedClaim{
			UID:                     uid,
			Namespace:               claim.Namespace,
			Name:                    claim.Name,
			Devices:                 devices,
			CPUConfig:               cpuConfig,
			State:                   state,
			OffersSplitAlternatives: offersSplit,
			Shape:                   shape,
		}
	}

	var newClaims []v1alpha1.ProjectedClaim
	for _, pc := range allocatedByUID {
		newClaims = append(newClaims, pc)
	}
	for _, oldPC := range existingProjected.Claims {
		if _, stillAllocated := allocatedByUID[oldPC.UID]; !stillAllocated {
			if oldPC.State == v1alpha1.ClaimStateAllocated {
				deallocated := oldPC
				deallocated.State = v1alpha1.ClaimStateDeallocated
				newClaims = append(newClaims, deallocated)
			}
		}
	}
	sort.Slice(newClaims, func(i, j int) bool {
		return newClaims[i].UID < newClaims[j].UID
	})

	if cmExists && lastGen > 0 && reflect.DeepEqual(newClaims, existingProjected.Claims) {
		return ctrl.Result{}, nil
	}

	newGen := lastGen + 1
	if newGen <= 0 {
		newGen = 1
	}
	projected := v1alpha1.ProjectedClaims{
		APIVersion: v1alpha1.APIVersion,
		Generation: newGen,
		Claims:     newClaims,
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("marshal projected claims for node %s: %w", nodeName, err)
	}

	if cmExists {
		if existingCM.Data == nil {
			existingCM.Data = make(map[string]string)
		}
		existingCM.Data[v1alpha1.ProjectedClaimsKey] = string(encoded)
		if err := r.Update(ctx, &existingCM); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		newCM := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: r.DriverNamespace,
			},
			Data: map[string]string{
				v1alpha1.ProjectedClaimsKey: string(encoded),
			},
		}
		if err := r.Create(ctx, &newCM); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}
