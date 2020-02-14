/*
利用sample-controller的框架，把业务代码填上
 */
package controller

import (
	"errors"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"math/rand"
	"time"
)

const (
	schedulerName = "random-scheduler"
	controllerAgentName = "random-scheduler"

	SuccessSynced = "Synced"
	MessageResourceSynced = "Pod synced successfully"
)

type predicateFunc func(node *corev1.Node, pod *corev1.Pod) bool
type priorityFunc func(node *corev1.Node, pod *corev1.Pod) int

type Scheduler struct {
	clientset  kubernetes.Interface
	podQueue   chan *corev1.Pod
	nodeLister corelistersv1.NodeLister
	nodeSynced cache.InformerSynced
	podLister  corelistersv1.PodLister
	podSynced  cache.InformerSynced
	workqueue workqueue.RateLimitingInterface
	recorder record.EventRecorder
	predicates []predicateFunc
	priorities []priorityFunc
}

func NewController(kubeclientset kubernetes.Interface,
	nodeInformer coreinformers.NodeInformer,
	podInformer coreinformers.PodInformer) *Scheduler {

	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	scheduler := &Scheduler{
		clientset:  kubeclientset,
		nodeLister: nodeInformer.Lister(),
		nodeSynced: nodeInformer.Informer().HasSynced,
		podLister:  podInformer.Lister(),
		podSynced:  podInformer.Informer().HasSynced,
		workqueue:  workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Random"),
		recorder:   recorder,
		predicates: []predicateFunc{
			randomPredicate,
		},
		priorities: []priorityFunc{
			randomPriority,
		},
	}

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				klog.Infoln("this is not a node")
				return
			}
			klog.Infof("New Node Added to Store: %s", node.GetName())
		},
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: scheduler.handleObject,
	})

	return scheduler
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (s *Scheduler) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer s.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting random scheduler")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, s.nodeSynced, s.podSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch two workers to process Foo resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(s.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (s *Scheduler) runWorker() {
	for s.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (s *Scheduler) processNextWorkItem() bool {
	obj, shutdown := s.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer s.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			s.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := s.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			s.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		s.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Foo resource
// with the current status of the resource.
func (s *Scheduler) syncHandler(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	pod, err := s.clientset.CoreV1().Pods(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	node, err := s.findFit(pod)
	if err != nil {
		klog.Errorf("cannot find node that fits pod", err.Error())
		return err
	}

	err = s.bindPod(pod, node)
	if err != nil {
		klog.Errorf("failed to bind pod", err.Error())
		return err
	}

	s.recorder.Event(pod, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (s *Scheduler) handleObject(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Infoln("this is not a pod")
		return
	}
	if pod.Spec.NodeName == "" && pod.Spec.SchedulerName == schedulerName {
		s.enqueue(obj)
	}
}

func (s *Scheduler) enqueue(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	s.workqueue.Add(key)
}

// 寻找最适合的node
func (s *Scheduler) findFit(pod *corev1.Pod) (string, error) {
	nodes, err := s.nodeLister.List(labels.Everything())
	if err != nil {
		return "", err
	}

	filteredNodes := s.runPredicates(nodes, pod)
	if len(filteredNodes) == 0 {
		return "", errors.New("failed to find node that fits pod")
	}
	priorities := s.prioritize(filteredNodes, pod)
	return s.findBestNode(priorities), nil
}

// pod绑定node
func (s *Scheduler) bindPod(p *corev1.Pod, node string) error {
	return s.clientset.CoreV1().Pods(p.Namespace).Bind(&corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		Target: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node,
		},
	})
}

func (s *Scheduler) runPredicates(nodes []*corev1.Node, pod *corev1.Pod) []*corev1.Node {
	filteredNodes := make([]*corev1.Node, 0)
	for _, node := range nodes {
		if s.predicatesApply(node, pod) {
			filteredNodes = append(filteredNodes, node)
		}
	}
	klog.Infoln("nodes that fit:")
	for _, n := range filteredNodes {
		klog.Infoln(n.Name)
	}
	return filteredNodes
}

func (s *Scheduler) predicatesApply(node *corev1.Node, pod *corev1.Pod) bool {
	for _, predicate := range s.predicates {
		if !predicate(node, pod) {
			return false
		}
	}
	return true
}

func randomPredicate(node *corev1.Node, pod *corev1.Pod) bool {
	r := rand.Intn(2)
	return r == 0
}

func (s *Scheduler) prioritize(nodes []*corev1.Node, pod *corev1.Pod) map[string]int {
	priorities := make(map[string]int)
	for _, node := range nodes {
		for _, priority := range s.priorities {
			priorities[node.Name] += priority(node, pod)
		}
	}
	klog.Infoln("calculated priorities:", priorities)
	return priorities
}

func (s *Scheduler) findBestNode(priorities map[string]int) string {
	var maxP int
	var bestNode string
	for node, p := range priorities {
		if p > maxP {
			maxP = p
			bestNode = node
		}
	}
	return bestNode
}

func randomPriority(node *corev1.Node, pod *corev1.Pod) int {
	return rand.Intn(100)
}


