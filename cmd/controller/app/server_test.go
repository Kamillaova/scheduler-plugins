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

package app

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	schedulingv1a1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

type fakeRESTMapper struct {
	meta.RESTMapper
	mapping *meta.RESTMapping
	err     error
}

func (f *fakeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return f.mapping, f.err
}

func TestIsCRDPresent(t *testing.T) {
	gvk := schedulingv1a1.SchemeGroupVersion.WithKind("PodGroup")

	presentMapper := &fakeRESTMapper{
		mapping: &meta.RESTMapping{
			Resource: gvk.GroupVersion().WithResource("podgroups"),
		},
	}
	present, err := isCRDPresent(presentMapper, gvk)
	require.NoError(t, err)
	require.True(t, present)

	absentMapper := &fakeRESTMapper{
		err: &meta.NoKindMatchError{
			GroupKind:        gvk.GroupKind(),
			SearchedVersions: []string{gvk.Version},
		},
	}
	present, err = isCRDPresent(absentMapper, gvk)
	require.NoError(t, err)
	require.False(t, present)

	errMapper := &fakeRESTMapper{
		err: errors.New("network failure"),
	}
	present, err = isCRDPresent(errMapper, gvk)
	require.Error(t, err)
	require.False(t, present)
}

func TestServerRunOptionsProjectorDefaults(t *testing.T) {
	opts := NewServerRunOptions()
	require.False(t, opts.EnableClaimProjector)
	require.Equal(t, "default", opts.DriverNamespace)
	require.Equal(t, "dra.cpu", opts.DriverName)
}
