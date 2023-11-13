"""
This script is used for copying stats wiki of a digital health study
it will prompt user target id (which health summary to build dashboard on),
and the desired synapseformation template,
and the wiki id of the dashboard to copy
Author: aryton.tediarjo@sagebase.org
"""
import sys
import pandas as pd
import logging
import argparse
from yaml import safe_load

import synapseclient
import synapseutils
from synapseclient import File, EntityViewSchema, Column, EntityViewType
from synapseformation import client as synapseformation_client

TEMPLATE_PATH = "synapseformation/manuscript.yaml"

syn = synapseclient.login()
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_project_id(syn, project_name):
    logger.info(f'Getting Synapse project id for {project_name}')
    response = syn.findEntityId(project_name)
    return "" if response is None else response


def create_project(syn, template_path, project_name):

    logger.info(
        f'Creating Synapse project {project_name}, ' + f'with template_path {template_path}')
    try:
        response = synapseformation_client.create_synapse_resources(
            template_path)
        logger.debug(f'Project response: {response}')
        if response is not None:
            return response.get('id')
    except Exception as e:
        logger.error(e)
        sys.exit(1)


def create_file_view(project_id):
    view = EntityViewSchema(
        name="mPower2.0 - File View",
        columns=[Column(name="task",
                        columnType="STRING"),
                 Column(name="analysisType",
                        columnType="STRING"),
                 Column(name="analysisSubtype",
                        columnType="STRING"),
                 Column(name="pipelineStep",
                        columnType="STRING"),
                 Column(name="consortium",
                        columnType="STRING"),
                 Column(name="study",
                        columnType="STRING"),
                 Column(name="studyOrProject",
                        columnType="STRING"),
                 Column(name="sensorType",
                        columnType="STRING_LIST"),
                 Column(name="deviceType",
                        columnType="STRING_LIST"),
                 Column(name="devicePlatform",
                        columnType="STRING_LIST"),
                 Column(name="dataCollectionMethod",
                        columnType="STRING_LIST"),
                 Column(name="deviceLocation",
                        columnType="STRING_LIST"),
                 Column(name="diagnosis",
                        columnType="STRING_LIST"),
                 Column(name="reportedOutcome",
                        columnType="STRING_LIST"),
                 Column(name="digitalAssessmentCategory",
                        columnType="STRING_LIST"),
                 Column(name="digitalAssessmentDetails",
                        columnType="STRING_LIST"),
                 Column(name="dataType",
                        columnType="STRING"),
                 Column(name="dataSubtype",
                        columnType="STRING"),
                 Column(name="dhPortalIndex",
                        columnType="BOOLEAN"),
                 Column(name="dataDescriptionLocation",
                        columnType="STRING"),
                 Column(name="dataAccessInstructions",
                        columnType="STRING"),
                Column(name="filter",
                        columnType="STRING"),
                Column(name="tool",
                        columnType="STRING")],
        parent=project_id,
        scopes=project_id,
        includeEntityTypes=[EntityViewType.FILE, EntityViewType.FOLDER],
        addDefaultViewColumns=True)
    view = syn.store(view)


def main():
    # get argument
    template_path = TEMPLATE_PATH

    # get data
    with open(template_path, 'r') as f:
        yaml_data = pd.json_normalize(safe_load(f))

    project_name = yaml_data["name"].iloc[0]

    # get project_id
    project_id = get_project_id(syn, project_name)
    print(project_id)

    # if there's a project id, assume the project is already connected to synapse
    connected_to_synapse = True if project_id else False

    # if no project id is available, create a new project
    if project_id == '':
        create_project(syn, template_path, project_name)
        project_id = get_project_id(syn, project_name)
        create_file_view(project_id)

if __name__ == "__main__":
    main()
