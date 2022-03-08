package scheduler

import (
	"encoding/json"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

var (
	KubeConfigFilePath = "/etc/kubernetes/scheduler.conf"
)

// BuildInClusterClientSet creates an in-cluster ClientSet
func BuildInClusterClientset() (*kubernetes.Clientset, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Errorf("Failed to build cluster config: %v", err)
		return nil, err
	}

	return kubernetes.NewForConfig(cfg)
}

// BuildClusterClientSet create cluster(for static pod) ClientSet
func BuildClusterClientSet() (*kubernetes.Clientset, error) {
	kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: KubeConfigFilePath},
		&clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		klog.Errorf("Failed to build cluster config: %v", err)
		return nil, err
	}
	kubeConfig.DisableCompression = true
	return kubernetes.NewForConfig(kubeConfig)

}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

// PrettyPrintJSON for debug
func PrettyPrintJSON(v interface{}) {
	prettyJSON, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		fmt.Printf("Failed to generate json: %s\n", err.Error())
	}
	fmt.Printf("%s\n", string(prettyJSON))
}
