package common

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	restcfg "sigs.k8s.io/controller-runtime/pkg/client/config"
)

// NewKubeClient creates an in-cluster Kubernetes client with KubeVirt backup types registered.
// Extracted as a variable to allow overriding in tests.
var NewKubeClient = func() (client.Client, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := kubevirtbackupv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register KubeVirt backup scheme: %w", err)
	}
	if err := kubevirtcorev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register KubeVirt scheme: %w", err)
	}

	restConfig, err := restcfg.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s client: %w", err)
	}

	return k8sClient, nil
}
