# -*- coding: utf-8 -*-
# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Google Cloud Logging functionalities."""

from typing import TYPE_CHECKING, List, Dict, Any, Optional

from libcloudforensics.providers.gcp.internal import common
from libcloudforensics import logging_utils  # pylint: disable=ungrouped-imports

if TYPE_CHECKING:
  import googleapiclient

logging_utils.SetUpLogger(__name__)
logger = logging_utils.GetLogger(__name__)

class GoogleCloudLog:
  """Class representing a Google Cloud Logs interface.

  Attributes:
    project_id: Google Cloud project ID.
    gcl_api_client: Client to interact with GCP logging API.

  Example use:
    # pylint: disable=line-too-long
    gcp = GoogleCloudLog(project_id='your_project_name')
    gcp.ListLogs()
    gcp.ExecuteQuery(filter='resource.type="gce_instance" labels."compute.googleapis.com/resource_name"="instance-1"')
    See https://cloud.google.com/logging/docs/view/advanced-queries for filter details.
  """

  LOGGING_API_VERSION = 'v2'

  def __init__(self, project_id: str, key_file: Optional[str] = None, search_all: bool = False) -> None:
    """Initialize the GoogleCloudProject object.

    Args:
      project_id (str): The name of the project.
    """

    self.project_id = project_id
    self.key_file = key_file
    self.gcl_api_client = None
    self.search_all = search_all
    self.projects = []

  def GclApi(self) -> 'googleapiclient.discovery.Resource':
    """Get a Google Compute Logging service object.

    Returns:
      googleapiclient.discovery.Resource: A Google Compute Logging service
          object.
    """

    if self.gcl_api_client:
      return self.gcl_api_client
    self.gcl_api_client = common.CreateService(
        'logging', self.LOGGING_API_VERSION, self.key_file)

    self.projects = []

    return self.gcl_api_client

  def ListProjects(self) -> None:
    self.projects = common.GetProjects(self.LOGGING_API_VERSION, self.key_file)

  def ListLogs(self) -> List[str]:
    """List logs in project.

    Returns:
      List[str]: The project logs available.

    Raises:
      RuntimeError: If API call failed.
    """

    logs = []
    responses = []
    project_ids = []
    gcl_instance_client = self.GclApi().logs()

    if self.search_all:
      if not self.projects:
        self.ListProjects()

      for p in self.projects:
        project_ids.append(p)
    else:
      project_ids = [self.project_id]

    for project in self.projects:
      if not project.get('lifecycleState') == 'ACTIVE':
        continue

      responses.append(common.ExecuteRequest(
          gcl_instance_client, 'list', {'parent': 'projects/' + project.get('projectId')}))

    for response in responses:
      for entry in response:
        for logtypes in entry.get('logNames', []):
          logs.append(logtypes)

    return logs

  def ExecuteQuery(self, qfilter: str) -> List[Dict[str, Any]]:
    """Query logs in one or more GCP projects.

    Args:
      qfilter (str): The query filter to use.

    Returns:
      List[Dict]: Log entries returned by the query, e.g. [{'projectIds':
          [...], 'resourceNames': [...]}, {...}]

    Raises:
      RuntimeError: If API call failed.
    """

    project_ids = []
    if self.search_all:
      if not self.projects:
        self.ListProjects()

      for p in self.projects:

        if not p.get('lifecycleState') == 'ACTIVE':
          continue

        project_ids.append('projects/{}'.format(p.get('projectId')))
    else:
      project_ids = ['projects/{}'.format(self.project_id)]

    total_projects = len(project_ids)
    logger.info("Projects being queried: {0:d}".format(total_projects))
    for x in range(0, len(project_ids), 50):
      body = {
          'resourceNames': project_ids[x:x+50] ,
          'filter': qfilter,
          'orderBy': 'timestamp desc',
      }

      gcl_instance_client = self.GclApi().entries()
      responses = common.ExecuteRequest(
          gcl_instance_client, 'list', {'body': body}, throttle=True)

      for response in responses:
        for entry in response.get('entries', []):
          yield entry
