from pyspark.sql import SparkSession
import axs
from axs import Constants
import os
import datetime

from kubernetes import client, config
import json
import os
import getpass
import time

from threading import Thread

from IPython.core.display import display, clear_output, HTML

class Writer():
    class Event():
        label = None
        html = None
        
        def __init__(self, html, label):
            self.label = label
            self.html = html
    
    events = []
    
    def append(self, html, label):
        event = self.Event(html, label)
        self.events.append(event)
        
    def prepend(self, html, label):
        event = self.Event(html, label)
        temp = self.events
        self.events = [event]
        for _ in temp:
            self.events.append(_)
    
    def delete_all_with_label(self, label):
        self.events = [event for event in self.events if event.label != label]
    
    def get_events(self):
        return self.events
    
    def set_events(self, events):
        self.events = events
    
    def update(self):
        clear_output(wait=True)
        display(HTML(" ".join([event.html for event in self.events])))
        
    def __init__(self):
        return None
        
class KubernetesComm():
    kube_client = None
    current_namespace = None
    current_user = None
    keep_polling = True
    time_sleep = 3
    
    pod_name = None

    def clear(self):
        clear_output(wait=True)

    def write(self, output):
        display(output)

    def refresh_kube_client(self):
        print("Starting or refreshing Kubernetes API Client")
        # Configs can be set in Configuration class directly or using helper utility
        try:
            config.load_kube_config(config_file=os.environ["KUBECONFIG"])
        except:
            config.load_incluster_config()

        try:
            self.current_namespace = os.environ["KUBENAMESPACE"]
        except:
            self.current_namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()

        try:
            self.current_user = os.environ["NB_USER"]
        except:
            self.current_user = getpass.getuser()

        self.kube_client = client.CoreV1Api()

    def poll(self):
        while self.keep_polling:
            try:
                pods = self.kube_client.list_namespaced_pod(self.current_namespace).to_dict()
            except Exception as e:
                print(e)
                self.refresh_kube_client()
                pods = self.kube_client.list_namespaced_pod(self.current_namespace).to_dict()

            spark_pods = [pod for pod in pods['items'] if self.pod_name in pod['metadata']['name']]
            pod_phases = ["Pending", "Running", "Succeeded", "Failed", "Unknown"]
            spark_pods_phases = {phase : [pod for pod in spark_pods if pod['status']['phase'] == phase ] 
                                for phase in pod_phases}
            spark_uids = { phase : [ pod['metadata']['uid'] for pod in items] for phase, items in spark_pods_phases.items()}

            events = self.kube_client.list_namespaced_event(self.current_namespace).to_dict()

            spark_events_phases = { phase : [event for event in events['items'] if event['involved_object']['uid'] in uids]
                                for phase, uids in spark_uids.items()}

            spark_pod_events = { phase : { pod['metadata']['name'] : [event for event in spark_events_phases[phase] if event['involved_object']['uid'] == spark_uids[phase][i]]
                    for i, pod in enumerate(pods) } for phase, pods in spark_pods_phases.items() }

            messages = { phase : { name : events[-1]['message'] for name, events in spark_pod_events[phase].items() } 
                for phase in spark_pod_events.keys()
            }
            # output = "\n\t".join([f"{key}: {value}" for key, value in messages.items()])
            self.clear()

            self.writer.delete_all_with_label("k8s-comm")
            self.writer.append(f"<p>Updated at: {datetime.datetime.now()}</p>", "k8s-comm")
            for phase, events in messages.items():
                if len(events) != 0:
                    self.writer.append("<h3>" + phase + "</h3>", "k8s-comm")
                    self.writer.append("<table>", "k8s-comm")

                    self.writer.append("<tr>", "k8s-comm")
                    self.writer.append("<th>Pod Name</th>", "k8s-comm")
                    self.writer.append("<th>Status</th>", "k8s-comm")
                    self.writer.append("</tr>", "k8s-comm")

                    for pod, event in events.items():
                        self.writer.append("<tr style='overflow-x: scroll;'>", "k8s-comm")
                        self.writer.append("<td><strong>" + pod + "</strong></td>", "k8s-comm")
                        self.writer.append("<td>" + event + "</td>", "k8s-comm")
                        self.writer.append("</tr>", "k8s-comm")

                    self.writer.append("</table>", "k8s-comm")

            self.writer.update()
            
            time.sleep(self.time_sleep)

    def __init__(self, **kwargs):
        self.refresh_kube_client()
        
        try:
            self.pod_name = kwargs['pod_name']
            print(f"set pod_name to '{self.pod_name}'")
        except Exception as e:
            self.pod_name = f"{self.current_user}-spark"

        try:
            self.time_sleep = kwargs['time_sleep']
        except Exception as e:
            pass
        
        try:
            self.writer = kwargs['writer']
        except Exception as e:
            self.writer = Writer
            
        return None

class DataBase():
    spark_context = None
    spark_session = None
    catalogs = None
    executor_prefix = None

    kc = None
    
    writer = None
    
    dirac_conf = {}
    
    dirac_catalogs = { "allwise" : "allwise", 
                      "gaiadr2" : "gaiadr2", 
                      "sdss" : "sdss", 
                      "ztfsample" : "cesium-speedtest-ztfsample",
                      "ztf_mar19" : "ztf_mar19"
                     }

    def init_conf(self, user_conf):
        self.dirac_conf['spark.executor.instances'] = 2
        
        try:
            username = os.environ["NB_USER"]
            time = int((datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds())
            self.executor_prefix = f"pyspark-{time}-{username}-spark"
            self.dirac_conf['spark.kubernetes.executor.podNamePrefix'] = self.executor_prefix
        except Exception as e:
            print(e)

        if user_conf:
            for key, value in user_conf.items():
                self.dirac_conf[key] = value

    def init_catalogs(self):
        if (self.catalogs):
            current_tables = self.catalogs.list_tables()
            for catalog_name, catalog_path in self.dirac_catalogs.items():
                try:
                    current_tables[catalog_name]
                    self.writer.append("<p>Found " + catalog_name + " in AXS catalogs.</p>", "dirac-status")
                    self.writer.update()
                except KeyError as e:
                    self.writer.append("<p>Adding " + catalog_name + " to AXS catalogs...</p>", "dirac-status")
                    self.writer.update()
                    try:
                        self.catalogs.import_existing_table(catalog_name, f's3a://axscatalog/{catalog_path}', num_buckets=500,
                                                            zone_height=Constants.ONE_AMIN, import_into_spark=True)
                    except AttributeError as axs_e:
                        self.writer.append("<p>" + str(axs_e) + "</p>", "dirac-status")
                        self.writer.update()
        else:
            self.writer.append("<p>AXS catalog not instantiated.</p>", "dirac-status")
            self.writer.update()
            return None
    
    def init_kubecomm(self, **kwargs):
        self.kc = KubernetesComm(**kwargs)

    def start(self, conf=None):
        self.init_conf(conf)
        self.init_kubecomm(pod_name=self.executor_prefix, writer=self.writer)
        
        self.writer.update()
        self.writer.append("<h2>Creating SparkSession</h2>", "dirac-start-status")
        self.writer.update()
        
        _spark_session = SparkSession.builder
        for key, value in self.dirac_conf.items():
            self.writer.append(f"<p>Setting config: {key}={value}<p>", "dirac-start-status")
            self.writer.update()
            _spark_session = _spark_session.config(str(key), str(value))
        self.spark_session = _spark_session

        self.writer.append(f"<h3> Spark Cluster Dashboard: <a href={self.get_spark_url()}>{self.get_spark_url()}</a></h3>", "dirac-start-link")
        self.writer.update()

        self.writer.append("<h2>Spark Cluster Status</h2>", "dirac-start-status")
        self.writer.update()
        k8s_poll_thread = Thread(target=self.kc.poll)
        k8s_poll_thread.start()
        
        self.spark_session = _spark_session.enableHiveSupport().getOrCreate()
        
        self.writer.update()
        self.kc.keep_polling = False
        k8s_poll_thread.join()
        self.writer.append("<p>Spark Cluster Created!</p>", "dirac-start-status")
        
        self.spark_context = self.spark_session.sparkContext
        
        self.writer.append("<h2>Loading AXS Catalogs</h2>", "dirac-start-status")
        self.catalogs = axs.AxsCatalog(self.spark_session)
        self.writer.update()
        self.init_catalogs()
            
    def stop(self):
        if self.spark_session:
            self.spark_session.stop()
        else:
            self.writer.append("<p>Spark cluster not created.</p>", "dirac-stop-status")
        
    def __init__(self, conf=None):
        self.writer = Writer()
        self.start(conf=conf)
        return None
        
    def load(self, table_name):
        return None
    
    def get_spark_context(self):
        if self.spark_context:
            return self.spark_context
        else:
            self.writer.append("<p>Spark cluster not created.</p>", "dirac-get-context-status")
            return None
        
    def get_spark_url(self):
        protocol = "https://"
        try:
            url = os.environ["SPARK_PUBLIC_DNS"]
        except:
            url = "localhost:4040/jobs/"
            
        return protocol + url
    
    def get_spark_session(self):
        if self.spark_session:
            return self.spark_session
        else:
            self.writer.append("<p>Spark cluster not created.</p>", "dirac-get-spark-session-status")
            return None
        
    def get_catalogs(self):
        if self.catalogs:
            return self.catalogs
        else:
            self.writer.append("<p>AXS catalog not instantiated.</p>", "dirac-get-catalogs-status")
            return None

def start(conf=None):
    return DataBase(conf=conf)

