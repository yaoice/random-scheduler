package main

import (
	"context"
	"flag"
	"github.com/yaoice/random-scheduler/pkg/client"
	"github.com/yaoice/random-scheduler/pkg/controller"
	"github.com/yaoice/random-scheduler/pkg/signals"
	corev1 "k8s.io/api/core/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"os"
	"time"
)

var (
	masterURL         string
	kubeconfig        string
	kubeClient        *kubernetes.Clientset
	err               error
	LockNameSpace     string
	LockName          string
	LockComponentName string
	LeaderElect       bool
	LeaseDuration     time.Duration
	RenewDeadline     time.Duration
	RetryPeriod       time.Duration
)

func main() {
	// 解析命令行参数
	klog.InitFlags(nil)
	flag.Parse()

	// 获取kubeClient
	kubeClient, err = client.GetInClusterClientSet()
	if err != nil {
		kubeClient, err = client.GetClusterClientSetWithKC(masterURL, kubeconfig)
		if err != nil {
			klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
		}
	}

	// 是否开启pod leader选举
	if !LeaderElect {
		run()
		return
	}

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typecorev1.EventSinkImpl{Interface: typecorev1.New(kubeClient.CoreV1().RESTClient()).Events(LockNameSpace)})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: LockComponentName})

	rl, err := resourcelock.New(
		resourcelock.EndpointsResourceLock,
		LockNameSpace,
		LockName,
		kubeClient.CoreV1(),
		kubeClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      hostname(),
			EventRecorder: recorder,
		})
	if err != nil {
		panic(err)
	}

	// Try and become the leader and start cloud controller manager loops
	leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: LeaseDuration,
		RenewDeadline: RenewDeadline,
		RetryPeriod:   RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: StartedLeading,
			OnStoppedLeading: StoppedLeading,
			OnNewLeader:      NewLeader,
		},
	})
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.BoolVar(&LeaderElect, "leader-elect", false, "Enable leader election,defalut value: true")
	flag.StringVar(&LockNameSpace, "leader-elect-namespace", "kube-system", "The resourcelock namespace,defalut value: operator")
	flag.StringVar(&LockName, "leader-elect-lock-name", "random-scheduler-leader-elect-lock", "The resourcelock name,defalut value: l5-controller-leader-elect-lock")
	flag.StringVar(&LockComponentName, "leader-elect-componentname", "leader-elector", "The resourcelock namespace,defalut value: leader-elector")
	flag.DurationVar(&LeaseDuration, "leader-elect-lease-duration", 15*time.Second, "The leader-elect LeaseDuration")
	flag.DurationVar(&RenewDeadline, "leader-elect-renew-deadline", 10*time.Second, "The leader-elect RenewDeadline")
	flag.DurationVar(&RetryPeriod, "leader-elect-retry-period", 2*time.Second, "The leader-elect RetryPeriod")
}

func StartedLeading(ctx context.Context) {
	klog.Infof("%s: started leading", hostname())
	run()
}

func run() {
	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	controller := controller.NewController(kubeClient,
		kubeInformerFactory.Core().V1().Nodes(),
		kubeInformerFactory.Core().V1().Pods())

	// notice that there is no need to run Start methods in a separate goroutine. (i.e. go kubeInformerFactory.Start(stopCh)
	// Start method is non-blocking and runs all registered informers in a dedicated goroutine.
	kubeInformerFactory.Start(stopCh)

	if err := controller.Run(2, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}
}

func StoppedLeading() {
	klog.Infof("%s: stopped leading", hostname())
}

func NewLeader(id string) {
	klog.Infof("%s: new leader: %s", hostname(), id)
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}
