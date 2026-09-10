/*
Copyright 2020 The Kubernetes Authors.

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

package app

import (
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2/klogr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	schedulingv1a1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/controllers"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(schedulingv1a1.AddToScheme(scheme))
	utilruntime.Must(resourceapi.AddToScheme(scheme))
}

func isCRDPresent(mapper meta.RESTMapper, gvk schema.GroupVersionKind) (bool, error) {
	_, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err == nil {
		return true, nil
	}
	if meta.IsNoMatchError(err) {
		return false, nil
	}
	return false, err
}

func Run(s *ServerRunOptions) error {
	config := ctrl.GetConfigOrDie()
	config.QPS = float32(s.ApiServerQPS)
	config.Burst = s.ApiServerBurst

	ctrl.SetLogger(klogr.New())
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: s.MetricsAddr,
		},
		HealthProbeBindAddress:  s.ProbeAddr,
		LeaderElection:          s.EnableLeaderElection,
		LeaderElectionID:        "sched-plugins-controllers",
		LeaderElectionNamespace: "kube-system",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}

	pgGVK := schedulingv1a1.SchemeGroupVersion.WithKind("PodGroup")
	hasPodGroup, err := isCRDPresent(mgr.GetRESTMapper(), pgGVK)
	if err != nil {
		setupLog.Error(err, "unable to check CRD presence", "crd", pgGVK.GroupKind().String())
		return err
	}
	if hasPodGroup {
		if err = (&controllers.PodGroupReconciler{
			Client:  mgr.GetClient(),
			Scheme:  mgr.GetScheme(),
			Workers: s.Workers,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "PodGroup")
			return err
		}
	} else {
		setupLog.Info("CRD not installed, skipping controller", "controller", "PodGroup")
	}

	eqGVK := schedulingv1a1.SchemeGroupVersion.WithKind("ElasticQuota")
	hasElasticQuota, err := isCRDPresent(mgr.GetRESTMapper(), eqGVK)
	if err != nil {
		setupLog.Error(err, "unable to check CRD presence", "crd", eqGVK.GroupKind().String())
		return err
	}
	if hasElasticQuota {
		if err = (&controllers.ElasticQuotaReconciler{
			Client:  mgr.GetClient(),
			Scheme:  mgr.GetScheme(),
			Workers: s.Workers,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ElasticQuota")
			return err
		}
	} else {
		setupLog.Info("CRD not installed, skipping controller", "controller", "ElasticQuota")
	}

	if s.EnableClaimProjector {
		if err = (&controllers.ClaimProjectorReconciler{
			Client:          mgr.GetClient(),
			Scheme:          mgr.GetScheme(),
			DriverNamespace: s.DriverNamespace,
			DriverName:      s.DriverName,
			Workers:         s.Workers,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "ClaimProjector")
			return err
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		return err
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}
	return nil
}
