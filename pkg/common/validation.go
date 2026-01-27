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

package common

import (
	"fmt"

	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
)

// VMReference contains the VM name and namespace extracted from a DataUpload.
type VMReference struct {
	Name      string
	Namespace string
}

// GetVMReference extracts the VirtualMachine reference from DataUpload annotations.
// It returns the VM name and namespace, or an error if the required annotation is missing.
// If the namespace annotation is not set, it defaults to the DataUpload's source namespace.
func GetVMReference(du *velerov2alpha1api.DataUpload) (*VMReference, error) {
	if du == nil {
		return nil, fmt.Errorf("DataUpload is nil")
	}

	annotations := du.GetAnnotations()
	if annotations == nil {
		return nil, fmt.Errorf("DataUpload %s/%s has no annotations", du.Namespace, du.Name)
	}

	vmName, ok := annotations[AnnotationVMName]
	if !ok || vmName == "" {
		return nil, fmt.Errorf("DataUpload %s/%s missing required annotation %s",
			du.Namespace, du.Name, AnnotationVMName)
	}

	// Namespace is optional - defaults to source namespace
	vmNamespace := annotations[AnnotationVMNamespace]
	if vmNamespace == "" {
		vmNamespace = du.Spec.SourceNamespace
	}

	return &VMReference{
		Name:      vmName,
		Namespace: vmNamespace,
	}, nil
}

// ValidateVMIsRunning checks if the VirtualMachine is in a running state.
// Returns nil if the VM is running, or an error describing why it's not.
func ValidateVMIsRunning(vm *kubevirtcorev1.VirtualMachine) error {
	if vm == nil {
		return fmt.Errorf("VirtualMachine is nil")
	}

	// Check PrintableStatus for running state
	if vm.Status.PrintableStatus != kubevirtcorev1.VirtualMachineStatusRunning {
		return fmt.Errorf("VirtualMachine %s/%s is not running (status: %s), offline backup not supported",
			vm.Namespace, vm.Name, vm.Status.PrintableStatus)
	}

	return nil
}

// ValidateCBTEnabled checks if Changed Block Tracking is enabled on the VirtualMachine.
// CBT must be enabled for incremental backups to work.
// Returns nil if CBT is enabled, or an error describing why it's not.
func ValidateCBTEnabled(vm *kubevirtcorev1.VirtualMachine) error {
	if vm == nil {
		return fmt.Errorf("VirtualMachine is nil")
	}

	// Check if ChangedBlockTracking status exists
	if vm.Status.ChangedBlockTracking == nil {
		return fmt.Errorf("VirtualMachine %s/%s does not have ChangedBlockTracking configured",
			vm.Namespace, vm.Name)
	}

	// Check if CBT is in Enabled state
	if vm.Status.ChangedBlockTracking.State != kubevirtcorev1.ChangedBlockTrackingEnabled {
		return fmt.Errorf("VirtualMachine %s/%s ChangedBlockTracking is not enabled (state: %s)",
			vm.Namespace, vm.Name, vm.Status.ChangedBlockTracking.State)
	}

	return nil
}

// ValidateVMForBackup performs all prerequisite checks for VM backup:
// 1. VM must be running
// 2. CBT must be enabled
// Returns nil if all checks pass, or an error describing the first failed check.
func ValidateVMForBackup(vm *kubevirtcorev1.VirtualMachine) error {
	// Check VM is running
	if err := ValidateVMIsRunning(vm); err != nil {
		return err
	}

	// Check CBT is enabled
	if err := ValidateCBTEnabled(vm); err != nil {
		return err
	}

	return nil
}
