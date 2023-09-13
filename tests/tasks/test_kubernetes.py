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

"""Test Task Kubernetes."""

from unittest.mock import patch

from pydolphinscheduler.tasks.kubernetes import Kubernetes
from constants import ImagePullPolicy, SelectorOperator


def test_kubernetes_get_define():
    """Test task kubernetes function get_define."""
    code = 123
    version = 1
    name = "test_kubernetes_get_define"
    image = "ds-dev"
    namespace = {"name": "default", "cluster": "lab"}
    min_cpu_cores = 2.0
    min_memory_space = 10.0
    ipp = ImagePullPolicy.ALWAYS
    command = ['bin']
    args = ['-m', 'foo']

    expect_task_params = {
        "resourceList": [],
        "localParams": [],
        "image": image,
        "namespace": str(namespace),
        "minCpuCores": min_cpu_cores,
        "minMemorySpace": min_memory_space,
        "dependence": {},
        "conditionResult": {"successNode": [""], "failedNode": [""]},
        "waitStartTimeout": {},
        'imagePullPolicy': ipp,
        'command': str(command),
        'args': str(args),
        'customizedLabels': [],
        'nodeSelectors': []
    }
    with patch(
            "pydolphinscheduler.core.task.Task.gen_code_and_version",
            return_value=(code, version),
    ):
        k8s = Kubernetes(name, image, ipp, namespace, min_cpu_cores, min_memory_space, command, args)
        assert k8s.task_params == expect_task_params


def test_kubernetes_get_define_with_optional_attr():
    """Test task kubernetes function get_define."""
    code = 123
    version = 1
    name = "test_kubernetes_get_define"
    image = "ds-dev"
    namespace = {"name": "default", "cluster": "lab"}
    min_cpu_cores = 2.0
    min_memory_space = 10.0
    ipp = ImagePullPolicy.ALWAYS
    command = ['bin']
    args = ['-m', 'foo']
    customized_labels = [{'label': 'user', 'value': 'admin'}, {'label': 'path', 'value': 'root'}]
    node_selectors = [{'key': 'foo', 'operator': SelectorOperator.DOES_NOT_EXIST, 'value': 'bar'}]

    expect_task_params = {
        "resourceList": [],
        "localParams": [],
        "image": image,
        "namespace": str(namespace),
        "minCpuCores": min_cpu_cores,
        "minMemorySpace": min_memory_space,
        "dependence": {},
        "conditionResult": {"successNode": [""], "failedNode": [""]},
        "waitStartTimeout": {},
        'imagePullPolicy': ipp,
        'command': str(command),
        'args': str(args),
        'customizedLabels': customized_labels,
        'nodeSelectors': node_selectors,
    }
    with patch(
            "pydolphinscheduler.core.task.Task.gen_code_and_version",
            return_value=(code, version),
    ):
        k8s = Kubernetes(name, image, ipp,namespace, min_cpu_cores, min_memory_space, command, args)
        for i in customized_labels:
            k8s.add_customized_label(i['label'], i['value'])
        for i in node_selectors:
            k8s.add_node_selector(i['key'], i['operator'], i['value'])

        assert k8s.task_params == expect_task_params


def test_kubernetes_get_define_without_optional_attrs():
    """Test task kubernetes function get_define."""
    code = 123
    version = 1
    name = "test_kubernetes_get_define"
    image = "ds-dev"
    ipp = ImagePullPolicy.ALWAYS

    expect_task_params = {
        "resourceList": [],
        "localParams": [],
        "image": image,
        "dependence": {},
        "conditionResult": {"successNode": [""], "failedNode": [""]},
        "waitStartTimeout": {},
        'imagePullPolicy': ipp,
        'customizedLabels': [],
        'nodeSelectors': [],
        'minCpuCores': None,
        'minMemorySpace': None,
        'namespace': None,
        'args': '[]',
        'command': '[]',
    }
    with patch(
            "pydolphinscheduler.core.task.Task.gen_code_and_version",
            return_value=(code, version),
    ):
        k8s = Kubernetes(name, image, ipp)

        assert k8s.task_params == expect_task_params
