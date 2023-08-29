from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import yaml
import json
import sys
from great_expectations.core.batch import BatchRequest
from great_expectations.cli import toolkit
from great_expectations.util import get_context
from great_expectations.cli.batch_request import get_batch_request, _get_data_asset_name_from_data_connector
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from great_expectations.exceptions import DataContextError

from results_db import write_to_database, query_success_rate


GX_ROOT_DIR = "/home/ubuntu/great_expectations"
app = Flask(__name__)
CORS(app)

@app.get('/testing')
def list_programming_languages():
    return {"message":"hello_world"}, 200

@app.get('/config')
def list_config():
    with open(GX_ROOT_DIR+"/great_expectations.yml") as yaml_in:
       yaml_object = yaml.safe_load(yaml_in)
    return jsonify(yaml_object), 200


@app.route('/config/<datasource_name>')
def get_datasource(datasource_name):
    with open(GX_ROOT_DIR+"/great_expectations.yml") as yaml_in:
        yaml_object = yaml.safe_load(yaml_in)
        yaml_object = yaml_object["datasources"][datasource_name]
    return jsonify(yaml_object), 200

@app.get('/expectations')
def list_expectations():
    expectations = [file.replace(".json", "") for file in os.listdir(GX_ROOT_DIR+"/expectations") if file.endswith(".json")]
    return {"result": expectations, "count": len(expectations)}, 200

@app.route('/expectations/<expectation_name>', methods=["GET","PUT","POST"])
def get_expectation(expectation_name):
    if request.method == "GET":
        with open(f"{GX_ROOT_DIR}/expectations/{expectation_name}.json") as json_in:
            json_object = json.load(json_in)
        return json_object, 200

    elif request.method == "PUT":
        context = get_context(context_root_dir=GX_ROOT_DIR)
        print("expect:", expectation_name)
        suite = context.get_expectation_suite(expectation_suite_name=expectation_name)
        print("suite OK")
        json_object = request.get_json()
        for expectation in json_object["expectations"]:
            expectation_configuration = ExpectationConfiguration(**expectation["expectation_configuration"])
            match expectation["action"]:
                case "add":
                    suite.add_expectation(expectation_configuration=expectation_configuration,
                                          match_type = "domain",
                                          overwrite_existing = False)
                case "edit":
                    suite.add_expectation(expectation_configuration=expectation_configuration,
                                          match_type = "domain",
                                          overwrite_existing = True)
                case "remove":
                    suite.remove_expectation(expectation_configuration=expectation_configuration,
                                             match_type = "domain",
                                             remove_multiple_matches = False)
                case _:
                    return {"status":"invalid action has been detected"}, 400

        context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=expectation_name)
        suite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=expectation_name)
        context.build_data_docs(resource_identifiers=[suite_identifier])
        return {"status":"success"}, 200

    elif request.method == "POST":
        context = get_context(context_root_dir=GX_ROOT_DIR)
        try:
            suite = context.get_expectation_suite(expectation_suite_name=expectation_name)
            return {"status": "error. expectation already exists"}, 400
        except DataContextError:
            suite = context.create_expectation_suite(expectation_suite_name=expectation_name)
        json_object = request.get_json()
        for expectation in json_object["expectations"]:
            expectation_configuration = ExpectationConfiguration(**expectation["expectation_configuration"])
            suite.add_expectation(expectation_configuration=expectation_configuration,
                                  match_type="success",
                                  overwrite_existing = True)
        context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=expectation_name)
        return {"status":"success"}, 200

@app.get('/checkpoints')
def list_checkpoints():
    checkpoints = [file.strip(".yml") for file in os.listdir(GX_ROOT_DIR+"/checkpoints")]
    return {"result": checkpoints, "count": len(checkpoints)}, 200

#TODO: HANDLE ERRORS FOR INCORRECT POST REQUEST FORMATS
#SWAP POST AND PUT WOOPS
@app.route('/checkpoints/<checkpoint_name>', methods=["GET","POST", "PUT"])
def get_checkpoint(checkpoint_name):
    try:
        with open(f"{GX_ROOT_DIR}/checkpoints/{checkpoint_name}.yml") as yaml_in:
            if request.method == "PUT":
                return {"result": "checkpoint already exists, to update please use the POST method"}, 400
            yaml_object = yaml.safe_load(yaml_in)
    except:
        if request.method == "PUT":
            pass
        else:
            return {"result":"checkpoint not found"}, 404
    if request.method == "GET":
        return jsonify(yaml_object), 200
    elif request.method == "POST":
        request_json = request.get_json()
        for key in request_json.keys():
            if key == "run_name_template" or key == "validations":
                yaml_object[key] = request_json[key]
        with open(f"{GX_ROOT_DIR}/checkpoints/{checkpoint_name}.yml", "w") as yaml_out:
            yaml.dump(yaml_object, yaml_out)
            return {"result":"success"}, 200
    elif request.method == "PUT":
        request_json = request.get_json()
        default = """action_list:
- action:
    class_name: StoreValidationResultAction
  name: store_validation_result
- action:
    class_name: StoreEvaluationParametersAction
  name: store_evaluation_params
- action:
    class_name: UpdateDataDocsAction
    site_names: []
  name: update_data_docs
batch_request: {}
class_name: Checkpoint
config_version: 1.0
evaluation_parameters: {}
expectation_suite_ge_cloud_id: null
expectation_suite_name: null
ge_cloud_id: null
module_name: great_expectations.checkpoint
profilers: []
run_name_template: 'TIMESTAMP_%Y-%m-%dT%H:%M:%S'
runtime_configuration: {}
template_name: null"""
        yaml_object = yaml.safe_load(default)
        yaml_object['name'] = checkpoint_name
        yaml_object['validations'] = request_json['validations']
        with open(f"{GX_ROOT_DIR}/checkpoints/{checkpoint_name}.yml", "w") as yaml_out:
            yaml.dump(yaml_object, yaml_out)
            return {"result":"success"}, 200

@app.get('/checkpoint_run')
def run_checkpoint():
    data_context_name=request.args.get("data_context") or GX_ROOT_DIR
    checkpoint_name = request.args.get("checkpoint_name") or ""
    if checkpoint_name == "":
        return {"error": "checkpoint name required in query parameters"}, 400
    context = get_context(context_root_dir=data_context_name)
    checkpoint = context.get_checkpoint(checkpoint_name)
    result = context.run_checkpoint(
        checkpoint_name = checkpoint_name,
        batch_request=None,
        run_name=None,
    )
    result = result.to_json_dict()
    write_to_database(result)
    return jsonify(result), 200

@app.get('/data_assets')
def get_data_assets():
    data_connector_name=request.args.get("data_connector_name") or "default_inferred_data_connector_name"
    datasource_name=request.args.get("datasource") or ""
    data_context_name=request.args.get("data_context") or GX_ROOT_DIR
    if datasource_name=="":
        return {"error": "datasource name required in query parameters"}, 400
    context = get_context(context_root_dir="/home/ubuntu/great_expectations")
    datasource = context.get_datasource(datasource_name=datasource_name)
    data_assets = datasource.get_available_data_asset_names(data_connector_names=data_connector_name)[data_connector_name]
    return {"datasource":datasource_name, "result": data_assets, "count": len(data_assets)}, 200

@app.route('/profile', methods=["PUT"])
def profile_data_asset():
    context = get_context(context_root_dir=GX_ROOT_DIR)
    json_object = request.get_json()
    expectation_suite_name = json_object["suite_name"]
    try:
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        return {"status": "error. expectation already exists"}, 400
    except DataContextError:
        suite = context.create_expectation_suite(expectation_suite_name=expectation_suite_name)

    try:
        data_connector_name = json_object["data_connector_name"]
    except KeyError:
        data_connector_name = "default_inferred_data_connector_name"
    batch_request = {"datasource_name": json_object["datasource_name"],
                     "data_connector_name": data_connector_name,
                     "data_asset_name": json_object["data_asset_name"],
                     "limit": 1000}
    exclude_column_names = json_object["exclude_columns"]

    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name
    )

    result = context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_names=exclude_column_names,
    )
    validator.expectation_suite = result.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator.save_expectation_suite(discard_failed_expectations=False)
    context.build_data_docs()
    return {"status":"success"}, 200

@app.get('/analytics/success_rate')
def get_success_rate():
    data_asset = request.args.get("data_asset")
    begin_date = request.args.get("begin")
    end_date = request.args.get("end")
    rate = query_success_rate(data_asset,begin_date,end_date)
    return {"status":"success", "percentage": rate*100}, 200


