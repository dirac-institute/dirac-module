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
        
    def clear(self):
        clear_output(wait=True)
    
    def update(self):
        self.clear()
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
            
            try:
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
            except Exception as e:
                # self.clear()
                self.writer("<p>Error getting Kubernetes Cluster events!</p><p>" + str(e) + "</p>", "k8s-comm")
                self.writer.update()

                time.sleep(self.time_sleep)
                continue
            
            # output = "\n\t".join([f"{key}: {value}" for key, value in messages.items()])
            # self.clear()

            self.writer.delete_all_with_label("k8s-comm")
            self.writer.append(f"<p>Updated at: {datetime.datetime.now()}</p>", "k8s-comm")

            num_events = sum([len(events) for phase, events in messages.items()])

            if num_events > 0:
                self.writer.append("<table>", "k8s-comm")
            
                # self.writer.append("<caption><strong>Pod Statuses</strong></caption>", "k8s-comm")

                self.writer.append("<tr>", "k8s-comm")
                self.writer.append("<th>Status</th>", "k8s-comm")
                self.writer.append("<th>Pod Name</th>", "k8s-comm")
                self.writer.append("<th>Message</th>", "k8s-comm")
                self.writer.append("</tr>", "k8s-comm")

                for phase, events in messages.items():
                    if len(events) != 0:                    
                        for pod, event in events.items():
                            self.writer.append("<tr style='overflow-x: scroll;'>", "k8s-comm")
                            if phase == "Pending":
                                self.writer.append("<td style='color: orange;'><strong>" + phase + "</strong></td>", "k8s-comm")
                            elif phase == "Running" or phase == "Succeeded":
                                self.writer.append("<td style='color: green;'><strong>" + phase + "</strong></td>", "k8s-comm")
                            elif phase == "Failed":
                                self.writer.append("<td style='color: red;'><strong>" + phase + "</strong></td>", "k8s-comm")
                            else:
                                self.writer.append("<td style='color: gray;'><strong>" + phase + "</strong></td>", "k8s-comm")

                            self.writer.append("<td>" + pod + "</td>", "k8s-comm")
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
                      "ztf" : "ztfmf1"
                     }

    def init_conf(self, user_conf):
        self.dirac_conf['spark.executor.instances'] = 2
        
        try:
            username = os.environ["NB_USER"]
            username_lower = username.lower()
            time = int((datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds())
            self.executor_prefix = f"pyspark-{time}-{username_lower}-spark"
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
                    self.writer.append("<p>Found " + catalog_name + " in AXS catalogs.</p>", "catalog-update")
                    self.writer.update()
                except KeyError as e:
                    self.writer.append("<p>Adding " + catalog_name + " to AXS catalogs...</p>", "catalog-update")
                    self.writer.update()
                    try:
                        self.catalogs.import_existing_table(catalog_name, f's3a://axscatalog/{catalog_path}', num_buckets=500,
                                                            zone_height=Constants.ONE_AMIN, import_into_spark=True)
                    except AttributeError as axs_e:
                        self.writer.append("<p>" + str(axs_e) + "</p>", "catalog-update")
                        self.writer.update()
        else:
            self.writer.append("<p>AXS catalog not instantiated.</p>", "catalog-update")
            self.writer.update()
            return None
    
    def init_kubecomm(self, **kwargs):
        self.kc = KubernetesComm(**kwargs)

    def start(self, conf=None):
        self.init_conf(conf)
        self.init_kubecomm(pod_name=self.executor_prefix, writer=self.writer)
        
        self.writer.clear()
        self.writer.append(f"<p><strong>Dashboard: <a href={self.get_spark_url()}>{self.get_spark_url()}</a></strong></p>", "dirac-start-link")
        self.writer.update()

        self.writer.update()
        self.writer.append("<p><strong>Status:</strong> Creating SparkSession </p>", "dirac-start-status")
        self.writer.update()
        
        _spark_session = SparkSession.builder
        for key, value in self.dirac_conf.items():
            self.writer.append(f"<p>Setting config: {key}={value}<p>", "dirac-start-status")
            self.writer.update()
            _spark_session = _spark_session.config(str(key), str(value))
        self.spark_session = _spark_session

        # time.sleep(1)
        # self.writer.delete_all_with_label("dirac-start-status")
        # self.writer.update()

        self.writer.append("<p><strong>Status:</strong> Polling Kubernetes cluster</p>", "dirac-start-status")
        self.writer.update()
        k8s_poll_thread = Thread(target=self.kc.poll)
        k8s_poll_thread.start()
        
        self.spark_session = _spark_session.enableHiveSupport().getOrCreate()
        
        self.writer.update()
        self.kc.keep_polling = False
        k8s_poll_thread.join()
        self.writer.append("<p><strong>Status:</strong> Spark cluster created!</p>", "dirac-start-status")

        # time.sleep(1)
        # self.writer.delete_all_with_label("dirac-start-status")
        # self.writer.delete_all_with_label("k8s-comm")
        # self.writer.update()

        self.spark_context = self.spark_session.sparkContext
        
        self.writer.append("<p><strong>Status:</strong> Loading AXS catalogs</p>", "catalog-update")
        self.catalogs = axs.AxsCatalog(self.spark_session)
        self.writer.update()
        # self.init_catalogs()

        # time.sleep(1)
        # self.writer.delete_all_with_label("catalog-update")
        # self.writer.append("<p><strong>Status:</strong> DataBase initialized!</p>", "dirac-start-status")
        # self.writer.update()

            
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
        protocol = "http://"

        try:
            username = os.environ["NB_USER"]
        except:
            username = "jovyan"
        
        sc = self.get_spark_context()
        port = sc.uiWebUrl.split(":")[-1]
        try:
            url = f"{protocol}{os.environ['PUBLIC_URL']}/user/{username}/proxy/{port}/jobs/"
        except:
            url = f"localhost:{port}/jobs/"
            
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

