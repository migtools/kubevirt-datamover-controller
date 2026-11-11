/*
Copyright 2026.

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

package controller

import (
	"context"
	"testing"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestDataUploadActivePhaseIndexer verifies that the active-phase indexer
// returns []string{"true"} only for DataUploads with DataMover == kubevirt
// AND Status.Phase in {Accepted, Prepared, InProgress}.
func TestDataUploadActivePhaseIndexer(t *testing.T) {
	tests := []struct {
		name      string
		dataMover string
		phase     velerov2alpha1.DataUploadPhase
		wantMatch bool
	}{
		{name: "kubevirt Accepted", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseAccepted, wantMatch: true},
		{name: "kubevirt Prepared", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhasePrepared, wantMatch: true},
		{name: "kubevirt InProgress", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseInProgress, wantMatch: true},
		{name: "kubevirt empty phase", dataMover: common.DataMoverKubeVirt, phase: "", wantMatch: false},
		{name: "kubevirt New", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseNew, wantMatch: false},
		{name: "kubevirt Completed", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseCompleted, wantMatch: false},
		{name: "kubevirt Failed", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseFailed, wantMatch: false},
		{name: "kubevirt Canceled", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseCanceled, wantMatch: false},
		{name: "kubevirt Canceling", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseCanceling, wantMatch: false},
		{name: "csi Accepted", dataMover: "csi", phase: velerov2alpha1.DataUploadPhaseAccepted, wantMatch: false},
		{name: "kopia InProgress", dataMover: "kopia", phase: velerov2alpha1.DataUploadPhaseInProgress, wantMatch: false},
		{name: "empty datamover Accepted", dataMover: "", phase: velerov2alpha1.DataUploadPhaseAccepted, wantMatch: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "ns"},
				Spec:       velerov2alpha1.DataUploadSpec{DataMover: tt.dataMover},
				Status:     velerov2alpha1.DataUploadStatus{Phase: tt.phase},
			}
			got := dataUploadActivePhaseIndexFunc(du)
			if tt.wantMatch {
				if len(got) != 1 || got[0] != "true" {
					t.Errorf("expected [\"true\"], got %v", got)
				}
			} else {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
			}
		})
	}
}

// TestDataUploadNonTerminalPhaseIndexer verifies that the non-terminal-phase
// indexer returns []string{"true"} only for DataUploads with DataMover == kubevirt
// AND Status.Phase in {"", New, Accepted, Prepared, InProgress}.
func TestDataUploadNonTerminalPhaseIndexer(t *testing.T) {
	tests := []struct {
		name      string
		dataMover string
		phase     velerov2alpha1.DataUploadPhase
		wantMatch bool
	}{
		{name: "kubevirt empty phase", dataMover: common.DataMoverKubeVirt, phase: "", wantMatch: true},
		{name: "kubevirt New", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseNew, wantMatch: true},
		{name: "kubevirt Accepted", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseAccepted, wantMatch: true},
		{name: "kubevirt Prepared", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhasePrepared, wantMatch: true},
		{name: "kubevirt InProgress", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseInProgress, wantMatch: true},
		{name: "kubevirt Completed", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseCompleted, wantMatch: false},
		{name: "kubevirt Failed", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseFailed, wantMatch: false},
		{name: "kubevirt Canceled", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseCanceled, wantMatch: false},
		{name: "kubevirt Canceling", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataUploadPhaseCanceling, wantMatch: false},
		{name: "csi empty phase", dataMover: "csi", phase: "", wantMatch: false},
		{name: "kopia New", dataMover: "kopia", phase: velerov2alpha1.DataUploadPhaseNew, wantMatch: false},
		{name: "empty datamover Accepted", dataMover: "", phase: velerov2alpha1.DataUploadPhaseAccepted, wantMatch: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "ns"},
				Spec:       velerov2alpha1.DataUploadSpec{DataMover: tt.dataMover},
				Status:     velerov2alpha1.DataUploadStatus{Phase: tt.phase},
			}
			got := dataUploadNonTerminalPhaseIndexFunc(du)
			if tt.wantMatch {
				if len(got) != 1 || got[0] != "true" {
					t.Errorf("expected [\"true\"], got %v", got)
				}
			} else {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
			}
		})
	}
}

// TestDataDownloadActivePhaseIndexer verifies that the active-phase indexer
// returns []string{"true"} only for DataDownloads with DataMover == kubevirt
// AND Status.Phase in {Accepted, Prepared, InProgress}.
func TestDataDownloadActivePhaseIndexer(t *testing.T) {
	tests := []struct {
		name      string
		dataMover string
		phase     velerov2alpha1.DataDownloadPhase
		wantMatch bool
	}{
		{name: "kubevirt Accepted", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseAccepted, wantMatch: true},
		{name: "kubevirt Prepared", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhasePrepared, wantMatch: true},
		{name: "kubevirt InProgress", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseInProgress, wantMatch: true},
		{name: "kubevirt empty phase", dataMover: common.DataMoverKubeVirt, phase: "", wantMatch: false},
		{name: "kubevirt New", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseNew, wantMatch: false},
		{name: "kubevirt Completed", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseCompleted, wantMatch: false},
		{name: "kubevirt Failed", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseFailed, wantMatch: false},
		{name: "kubevirt Canceled", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseCanceled, wantMatch: false},
		{name: "kubevirt Canceling", dataMover: common.DataMoverKubeVirt, phase: velerov2alpha1.DataDownloadPhaseCanceling, wantMatch: false},
		{name: "csi Accepted", dataMover: "csi", phase: velerov2alpha1.DataDownloadPhaseAccepted, wantMatch: false},
		{name: "kopia InProgress", dataMover: "kopia", phase: velerov2alpha1.DataDownloadPhaseInProgress, wantMatch: false},
		{name: "empty datamover Accepted", dataMover: "", phase: velerov2alpha1.DataDownloadPhaseAccepted, wantMatch: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dd := &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "ns"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: tt.dataMover},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: tt.phase},
			}
			got := dataDownloadActivePhaseIndexFunc(dd)
			if tt.wantMatch {
				if len(got) != 1 || got[0] != "true" {
					t.Errorf("expected [\"true\"], got %v", got)
				}
			} else {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
			}
		})
	}
}

// TestRegisterDataUploadActivePhaseIndex_Integration verifies end-to-end that
// RegisterDataUploadActivePhaseIndex registers the indexer correctly and that
// List with MatchingFields returns only matching DataUploads.
func TestRegisterDataUploadActivePhaseIndex_Integration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	matching := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-match", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseAccepted},
	}
	nonMatching := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-no-match", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseCompleted},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(matching, nonMatching).
		WithIndex(&velerov2alpha1.DataUpload{}, IndexFieldDataUploadActivePhase, dataUploadActivePhaseIndexFunc).
		Build()

	duList := &velerov2alpha1.DataUploadList{}
	if err := fakeClient.List(context.Background(), duList,
		client.InNamespace("ns"),
		client.MatchingFields{IndexFieldDataUploadActivePhase: "true"},
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(duList.Items) != 1 {
		t.Fatalf("expected 1 matching DU, got %d", len(duList.Items))
	}
	if duList.Items[0].Name != "du-match" {
		t.Errorf("expected du-match, got %s", duList.Items[0].Name)
	}
}

// TestRegisterDataUploadNonTerminalPhaseIndex_Integration verifies that
// RegisterDataUploadNonTerminalPhaseIndex registers correctly and includes
// empty-phase and New DataUploads (the broader set hasOlderActiveDUForVM needs).
func TestRegisterDataUploadNonTerminalPhaseIndex_Integration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	emptyPhase := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-empty", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataUploadStatus{Phase: ""},
	}
	newPhase := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-new", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseNew},
	}
	accepted := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-accepted", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseAccepted},
	}
	completed := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-completed", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseCompleted},
	}
	csiDU := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{Name: "du-csi", Namespace: "ns"},
		Spec:       velerov2alpha1.DataUploadSpec{DataMover: "csi"},
		Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseAccepted},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(emptyPhase, newPhase, accepted, completed, csiDU).
		WithIndex(&velerov2alpha1.DataUpload{}, IndexFieldDataUploadNonTerminalPhase, dataUploadNonTerminalPhaseIndexFunc).
		Build()

	duList := &velerov2alpha1.DataUploadList{}
	if err := fakeClient.List(context.Background(), duList,
		client.InNamespace("ns"),
		client.MatchingFields{IndexFieldDataUploadNonTerminalPhase: "true"},
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should match: emptyPhase, newPhase, accepted (3 items)
	// Should NOT match: completed (terminal), csiDU (wrong datamover)
	if len(duList.Items) != 3 {
		t.Fatalf("expected 3 matching DUs, got %d", len(duList.Items))
	}
}

// TestRegisterDataDownloadActivePhaseIndex_Integration verifies that
// RegisterDataDownloadActivePhaseIndex registers correctly and List with
// MatchingFields returns only matching DataDownloads.
func TestRegisterDataDownloadActivePhaseIndex_Integration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	matching := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "dd-match", Namespace: "ns"},
		Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
	}
	nonMatching := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "dd-no-match", Namespace: "ns"},
		Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
		Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseFailed},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(matching, nonMatching).
		WithIndex(&velerov2alpha1.DataDownload{}, IndexFieldDataDownloadActivePhase, dataDownloadActivePhaseIndexFunc).
		Build()

	ddList := &velerov2alpha1.DataDownloadList{}
	if err := fakeClient.List(context.Background(), ddList,
		client.InNamespace("ns"),
		client.MatchingFields{IndexFieldDataDownloadActivePhase: "true"},
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ddList.Items) != 1 {
		t.Fatalf("expected 1 matching DD, got %d", len(ddList.Items))
	}
	if ddList.Items[0].Name != "dd-match" {
		t.Errorf("expected dd-match, got %s", ddList.Items[0].Name)
	}
}
