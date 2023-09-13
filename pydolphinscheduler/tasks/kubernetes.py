# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Task Kubernetes."""
from pydolphinscheduler.constants import TaskType
from pydolphinscheduler.core.task import Task
from pydolphinscheduler.constants import ImagePullPolicy, SelectorOperator
from pydolphinscheduler import configuration
import json


class Kubernetes(Task):
    """Task Kubernetes object, declare behavior for Kubernetes task to dolphinscheduler.

    :param name: task name
    :param image: the registry url for image.
    :param namespace: the namespace for running Kubernetes task.
    :param min_cpu_cores: min CPU requirement for running Kubernetes task.
    :param min_memory_space: min memory requirement for running Kubernetes task.
    :param params_map: It is a local user-defined parameter for Kubernetes task.
    :param image_pull_policy: docker image pull policy
    :param command: docker runtime command
    :param args: docker runtime args
    :param customized_labels: docker customized labels
    :param node_selectors: docker node selector conditions
    """

    _task_custom_attr = {
        "image",
        "namespace",
        "min_cpu_cores",
        "min_memory_space",
        "image_pull_policy",
        "command",
        "args",
        "customized_labels",
        "node_selectors"
    }

    def __init__(
            self,
            name: str,
            image: str = configuration.KUBERNATES_IMAGE,
            image_pull_policy=ImagePullPolicy.ALWAYS,
            namespace: dict = configuration.KUBERNATES_NAMESPACE,
            min_cpu_cores: float = None,
            min_memory_space: float = None,
            command: list = None,
            exec_args: list = None,
            customized_labels: list = None,
            node_selectors: list = None,
            *args,
            **kwargs
    ):
        super().__init__(name, TaskType.KUBERNETES, *args, **kwargs)
        command = [] if command is None else command
        exec_args = [] if exec_args is None else exec_args
        customized_labels = [] if customized_labels is None else customized_labels
        node_selectors = [] if node_selectors is None else node_selectors

        self.image = image
        self.namespace = namespace
        self.min_cpu_cores = min_cpu_cores
        self.min_memory_space = min_memory_space
        self.image_pull_policy = image_pull_policy
        self.command = command
        self.args = exec_args
        self.customized_labels = customized_labels
        self.node_selectors = node_selectors

    def add_customized_label(self, label: str, value: str):
        self.customized_labels.append({'label': label, 'value': value})

    def add_node_selector(self, key: str, operator: SelectorOperator, value):
        self.node_selectors.append({'key': key, 'operator': operator, 'value': value})

    def _get_attr_wrappers(self):
        return {'namespace': json.dumps, 'args': json.dumps, 'command': json.dumps}
