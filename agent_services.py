import json, os, base64,io,requests,time, zipfile , uuid, copy
from typing import Any, Dict, Generator 
from datetime import datetime,date
from decimal import Decimal
from app.config import OrchConfig, OrchLogConfig
from app.core.custom_exception_func import (
    agent_draft_exception,
    agent_id_exception,
    configuration_exception,
    draft_status_exception,
    invalid_json_exception,
    invalid_mode,
    invalid_response,
    invalid_status,
    missing_field_exception,
    missing_value_exception,
    no_data_exception,
    usecase_code_exception,
    validation_error,
    sse_error_response,
    usecase_name_exception,
    file_not_found,
    invalid_file_exception,
    simulation_id_exception,
    extraction_id_exception
)

from app.core.custom_exceptions import (
    AgentDraftException,
    AgentInDraftStatus,
    IncompleteConfigurationError,
    InvalidAgentId,
    InvalidSimulationId,
    InvalidJson,
    InvalidMode,
    InvalidModelType,
    InvalidResponse,
    InvalidStatus,
    InvalidUseCaseCode,
    MissingFields,
    MissingValue,
    NoDataException,
    ValidationError,
    InvalidExtractionId,
    InvalidInput,
    FuncException,
    UseCaseNameMissMatch,
    UseCaseCodeException,
    InvalidFileException,
    FileNotFoundException,
    FileNotFound
)
from app.db.cache import get_cache, set_cache
from app.diet.db_loggers import retrive_simulate_id
from app.diet.diet_response import ServiceResponse, StatusData as sd
from app.diet.custom_exceptions import InvalidExtractID
from app.utils.validate_file import extract_json_files_from_zip, validate_zip_file
from app.db.db_loggers import (
    activate_agent,
    create_agent,
    create_simulate_hist,
    delete_agent,
    fetch_records_by_usecase_code_publish,
    fetch_tool_details,
    generate_transaction_id,
    get_master_data_records,
    get_published_records_by_agent_id,
    get_records_by_agent_id,
    get_records_by_usecase_code,
    get_simulate_hist_records_by_agent_id,
    get_simulation_hist_data,
    update_agent_details,
    update_context_config,
    update_data_embedding_config,
    update_guardrails_config,
    update_interface,
    update_llm_config,
    update_orch_config,
    update_publish_data,
    update_retrievers_config,
    update_simulate_hist,
    update_system_prompt,
    validate_simulate_input,
    get_simulate_details_by_sim_id,
    import_agent_details,
    get_available_files_by_agent_id
    
)
from app.db.extractions import update_extract_audit, update_extract_details,retrieve_filename, update_rte_extract_details, update_rte_extract_audit,create_audit_details,create_rte_audit_details
from app.schemas.agent_response import (
    AgentData,
    AgentId,
    AgentListData,
    AgentResponse,
    CacheConfigData,
    CacheData,
    CompData,
    FailedResponseData,
    MasterData,
    PublishData,
    SimulationHistData,
    SimulationDetails,
    StatusData,
    SubCompData,
    SubData,
    FileData,
    StatusData,
    FailedResponseData
)
from app.utils.coordinate_cleaner import remove_coordinates
from app.utils.get_redis_client import get_redis_client
from app.utils.log_config import setup_logger
from app.utils.postprocessing_middleware import CAOutputBuilder, final_preprocess
from app.utils.postprocessor import build_output
from app.utils.validate_input import (
    create_chat_payload,
    create_payload,
    create_payload_log,
    create_payload_publish,
    create_rte_payload,
    validate_input,
    create_rte_chat_payload
)
from flask import Response, jsonify, make_response, request, stream_with_context, send_file
from pathlib import Path
from app.schemas.agent_request import AgentRequest
from util.getuser import get_user_name
from app.db.context_management_db import get_groups_by_agent_id,fetch_file_content_by_agent_id
from app.db.mcp_tool_db import retrieve_mcp_server_details

from werkzeug.utils import secure_filename
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(BASE_DIR, "doc_seqa_interface.json")
logger = setup_logger(__name__)

from Security.lib.admin.publishModel import PublishModel

pub_obj = PublishModel()
INVALID_RESPONSE = "Not a Valid Response"
SIMULATION_COMPLETED="Simulation completed"
SIMULATION_FAILED="Simulation failed"
YIELD_RESPONSE ="data: [DONE]\n\n"

session_memory ={}


class AgentAPIService:

    def create_agent_service(self, data):
        try:
            logger.info("In Create Agent Service")
            data = create_agent(data)
            data = AgentId(agent_id=data)
            response = AgentResponse(
                data=data,
                status=StatusData(
                    code=0, message="New Agent Created"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except AgentDraftException as e:
            draft_excp = agent_draft_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return draft_excp
        except Exception as e:
            logger.exception(f"Error in Creating Agent Configurations(): {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in creating Agent Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_llm_config_service(self, data):
        try:
            logger.info("In Update LLM config Service ")
            update_llm_config(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully updated LLM Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating LLM Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Updating LLM Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_context_config_service(self, data):
        try:
            logger.info("In Update Context config Service")
            update_context_config(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully updated Context Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating Context Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Updating Context Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_orch_config_service(self, data):
        try:
            logger.info("In Update Orchestrator config Service")
            update_orch_config(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully updated Orchestrator Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except MissingValue as e:
            missing_value_excp = missing_value_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_value_excp
        except Exception as e:
            logger.exception(f"Error in Updating Orch Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Updating Orch Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_guardrails_config_service(self, data):
        try:
            logger.info("In Update Guardrails config Service")
            update_guardrails_config(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully updated Guardrails Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating Guardrails Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Updating Guardrails Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_retrievers_config_service(self, data):
        try:
            logger.info("In Update Retriever config Service")
            update_retrievers_config(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully updated Retriever Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating Retriever Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Updating Retriever Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_data_embedding_config_service(self, data):
        try:
            logger.info("In Update Data Embedding config Service")
            update_data_embedding_config(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully updated Data Embedding Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating Data Embedding Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Updating Data Embedding Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_system_prompt_service(self, data):
        try:
            logger.info("In Update System Prompt config Service")
            update_system_prompt(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=0, message="Successfully updated System Prompt"),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating System Prompt() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=1, message="Error in Updating System Prompt"),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def update_interface_service(self, data):
        try:
            logger.info("In Update Interface config Service")
            update_interface(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=0, message="Successfully updated Interface Configuration"),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating Interface Configurations() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in updating Interface Configurations"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def delete_agent_service(self, data):
        try:
            logger.info("In Delete config Service")
            delete_agent(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0, message="Successfully Deleted the Configuration"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except AgentInDraftStatus as e:
            draft_status_excp = draft_status_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return draft_status_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except Exception as e:
            logger.exception(f"Error in deleting Agent Configuration {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Deleting Agent Configuration"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def get_records_by_agent_id_service(self, data):
        try:
            logger.info("In fetch record based on agentid Service")
            result = get_records_by_agent_id(data)
            user_name = get_user_name(result["AD_CREATED_BY"], None)
            data = AgentData(
                agent_id=result["AD_ID"],
                usecase_code=result["AD_USECASE_CODE"],
                version=result["AD_VERSION"],
                status=result["AD_STATUS"],
                llm_config=result["AD_LLM_CONFIG"],
                context_config=result["AD_CONTEXT_CONFIG"],
                orch_config=result["AD_ORCH_CONFIG"],
                guardrails_config=result["AD_GUARDRAILS_CONFIG"],
                retrievers=result["AD_RETRIEVERS_CONFIG"],
                data_embeddings=result["AD_DATA_EMBEDDING_CONFIG"],
                system_prompt=result["AD_SYSTEM_PROMPT"],
                interface=result["AD_INTERFACE"],
                agent_config=result["AD_CONFIG"],
                api_key=result["AD_API_KEY"],
                agent_type=result["AD_AGENT_TYPE"],
                is_chat_enabled=result["AD_ISCHAT_ENABLED"],
                created_by=user_name,
                created_date=result["AD_CREATED_DATE"],
                updated_by=result["AD_UPDATED_BY"],
                updated_date=result["AD_UPDATED_DATE"],
            )
            response = AgentResponse(
                data=data,
                status=StatusData(
                    code=0, message="Agent Configuration Details based on AgentID"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except Exception as e:
            logger.exception(
                f"Error in Displaying Configuration Details based on AgentId {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Configuration Details based on AgentId",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def get_records_by_usecase_code_service(self, data):
        try:
            result = get_records_by_usecase_code(data)
            data_list = []
            for row in result:
                user_name = get_user_name(row.get("AD_CREATED_BY"), None)
                data = AgentListData(
                    agent_id=row.get("AD_ID"),
                    version=row.get("AD_VERSION"),
                    status=row.get("AD_STATUS"),
                    agent_config=row.get("AD_CONFIG"),
                    agent_type=row.get("AD_AGENT_TYPE"),
                    is_chat_enabled=row.get("AD_ISCHAT_ENABLED"),
                    created_by=user_name,
                    created_date=row.get("AD_CREATED_DATE")
                )
                data_list.append(data)
            response = AgentResponse(
                data=data_list,
                status=StatusData(
                    code=0,
                    message="List of Agents available",
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except Exception as e:
            logger.exception(f"Error in Displaying Agents {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Agents",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def _handle_final_stream(self,chunk_data,is_diet,result,data,simulation_id):
        data_ = chunk_data.get("data") or {}
        status = chunk_data.get("status") or {}
        logger.info(f"data_ is {data_}")
        annotation = data_.get("annotation")
        logger.info(f"annotation {annotation}")           
        if data.type == "cre":
            final_output = self._process_cre_output(data_,result,data,simulation_id,is_diet,annotation)
            logger.info(f"post process output {final_output}")
        else:
            final_output = self._process_rte_output(data_,result,is_diet)
        return final_output, status, annotation


    def _process_cre_output(self,data_,result,data,simulation_id,is_diet,annotation):
        if result["AD_INTERFACE"]["General"]["outputType"] == "markdown":
            logger.info(f"markdown {type(data_['output'])}")
            if data_["output"]:
                data_["output"]=data_["output"].replace("'",'"')
                output_data=json.loads(data_["output"])
                d1=output_data[0]["text"]
                update_simulate_hist(data,d1, simulation_id, "COMPLETED", annotation)
                return d1
        output_schema = result["AD_INTERFACE"]["Output"]
        output_values = data_["output"]
        if result["AD_AGENT_TYPE"] == "DOCUMENT_SEQA":
            is_diet = True
        start = time.perf_counter()
        logger.info(f'output_schema is {output_schema}')
        final_output = build_output(output_values, output_schema, data_, is_diet)
        end = time.perf_counter()
        logger.info(f"Time taken for postprocessing : {end - start:.6f} sec.")
        if result["AD_AGENT_TYPE"] == "GENERIC":
            update_simulate_hist(data, final_output, simulation_id, "COMPLETED", annotation)
        return final_output

    def _process_rte_output(self,data_,result,is_diet):
        if result["General"]["outputType"] == "markdown":
            logger.info("markdown")
            return data_["output"]

        output_schema = result["Output"]
        output_values = data_["output"]
        start = time.perf_counter()
        final_output = build_output(output_values, output_schema, data_, is_diet)
        end = time.perf_counter()
        logger.info(
            f"Time taken for postprocessing in runtime : {end - start:.6f} sec."
        )
        return final_output
    

    def extract_final_output_from_stream(self, is_diet, result, data, extraction_id=None,simulation_id=None, response=None):
        logger.info("In extract final output from stream")
        final_output = {}
        for line in response.iter_lines():
            if not line:
                continue
            line_str = line.decode("utf-8")
            if not line_str.startswith("data: "):
                continue
            data_str = line_str[6:]
            if data_str.strip() == "[DONE]":
                break
            try:
                chunk_data = json.loads(data_str)
                stream_type = chunk_data.get("type")
                if stream_type == "content":
                    continue
                if stream_type == "error":
                    logger.info('Error in Orchestration')
                    final_output, status, annotation = self._handle_stream_error(
                        chunk_data, data, simulation_id, final_output,extraction_id
                    )
                if stream_type == "final":
                    return self._handle_final_stream(
                        chunk_data, is_diet, result, data, simulation_id
                    )
            except Exception as e:
                logger.info(f"Error in extract final output from stream {e}")
                raise e
        return final_output, status, annotation


    def _handle_stream_error(self, chunk_data, data, simulation_id, final_output,extraction_id):
        logger.info("Error from orch")
        annotation = None
        status = chunk_data.get("status") or {}
        error_message = status.get("message")

        if data.type == "cre":
            logger.info("error in simulation")
            update_simulate_hist(
                data, None, simulation_id, "FAILED", None, error_message
            )
            update_extract_audit(data,None,extraction_id,"FAILED",annotation,error_message)
        elif data.type == "rte":
            update_rte_extract_audit(data,None,extraction_id,"FAILED",annotation,error_message)
        return final_output, status, annotation


    def diet_service(self,data,post_output,extraction_id,document_id,extract_name,created_on,filename,annotation,highlight=True):
        output_builder = CAOutputBuilder()
        output = output_builder.set_output_dynamic(post_output)
        diet_response = {'Entities':output}
        logger.info(f'final_output from diet_response is {diet_response}')
        if data.type=="cre":
            update_extract_audit(data,diet_response,extraction_id,"COMPLETED",annotation)
        elif data.type=="rte":
            update_rte_extract_audit(data,diet_response,extraction_id,"COMPLETED",annotation)
        diet_ui_response=final_preprocess(output,highlight,extraction_id,document_id,extract_name,created_on,filename,annotation)
        logger.info(f'final_output from diet_UI_servie is {diet_ui_response}')
        return diet_ui_response

    def simulate_config_service(self, bearer_token, data):
        return Response(
        stream_with_context(
            self._simulate_generator(bearer_token, data)
        ),
        headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
 
 
    def _simulate_generator(self, bearer_token, data):
        try:
            logger.info("Inside cre simulate service")
            headers = {"Authorization": f"{bearer_token}"}
            validate_simulate_input(data)
            result,mcp_data = fetch_records_by_usecase_code_publish(data)
            logger.info(f"mcp details to be stored in cache: {mcp_data}")
            orch_config = result["AD_ORCH_CONFIG"]
            orch_config = remove_coordinates(orch_config)
            cache_data = CacheData(
                agent_id=result["AD_ID"],
                agent_code=result["AD_AGENT_CODE"],
                usecase_code=result["AD_USECASE_CODE"],
                version=result["AD_VERSION"],
                status=result["AD_STATUS"],
                orch_config=orch_config,
                system_prompt=result["AD_SYSTEM_PROMPT"],
                interface=result["AD_INTERFACE"],
                agent_config=result["AD_CONFIG"],
                is_chat_enabled=result["AD_ISCHAT_ENABLED"],
                agent_type=result["AD_AGENT_TYPE"],
                api_key=result["AD_API_KEY"],
                mcp_config=mcp_data
            )
            redis_client = get_redis_client(data.type)
            key = f"CRE_AGV{cache_data.version}_UC_{cache_data.usecase_code}"
            logger.info(f"key is {key}")
            set_cache(key, cache_data, redis_client)
            if data.is_chat == True and result["AD_AGENT_TYPE"]== "DOCUMENT_SEQA":
                with open(json_path, "r") as f:
                    json_value = json.load(f)
                    interface=json_value
            else:
                interface= result["AD_INTERFACE"]["Input"]
            input_data = validate_input(data.input, interface)           
            logger.info("Validation Successfull")
            if result["AD_ISCHAT_ENABLED"] is True:
                payload, simulation_id, user_message = self.chat_simulate(
                    data, result, input_data
                )
                logger.info(f"cre payload is {payload}")
            else:
                simulation_id = create_simulate_hist(data, 'INPROGRESS', input_data)
                payload = create_payload(
                    data,
                    simulation_id,
                    result["AD_USECASE_CODE"],
                    result["AD_VERSION"],
                    input_data
                )
                user_message = {}

            start = time.perf_counter()
            init_chunk = {
                "type": "initiate",
                "data": {
                    "simulation_id": simulation_id,
                    "output": "Simulation initiated successfully"
                }
            }
            yield self._format_sse_chunk(init_chunk)

            environ = data.type
            if result["AD_ISCHAT_ENABLED"] is True and result["AD_AGENT_TYPE"] == "DOCUMENT_SEQA" and data.is_chat == False:
                entries = payload
                if not isinstance(entries, list):
                    entries = [{"payload": payload, "user_message": user_message}]
                
                final_outputs = []
                message = SIMULATION_COMPLETED
            
                success_count = 0

                for idx, entry in enumerate(entries, start=1):
                    per_payload   = entry.get("payload")
                    user_message  = entry.get("user_message", {})
                    extraction_id = entry.get("extraction_id")
                    document_id   = entry.get("document_id")
                    created_on    = entry.get("created_on")
                    extract_name  = entry.get("extract_name")
                    filename      = per_payload["input"][0][list(per_payload["input"][0].keys())[0]][0]["file_name"]

                    logger.info(f"Payload for orch is {per_payload}")
                    call_url = OrchConfig.URL.replace("mode", environ)

                    logger.info(f"call_url is {call_url}")
                    response = requests.post(call_url, headers=headers, json=per_payload, stream=True)
                    logger.info(f"Response from orch {response.text}")
                    response.raise_for_status()

                    final_output, status, annotation = self.extract_final_output_from_stream(result=result,is_diet=True,extraction_id=extraction_id, 
                                                                                            simulation_id=simulation_id,data=data,response=response)

                    logger.info(f"final output from postprocess is {final_output}\n, "
                        f"status is {status}, annotation is {annotation}")

                    if status["code"] == 0:
                        success_count += 1      
                        final_output = self.diet_service( data,final_output, extraction_id, document_id,extract_name,created_on,filename,annotation)
                        final_outputs.append(copy.deepcopy(final_output))
                    else:
                        logger.info(f'Orchestrator returned error code: {status["code"]}')

                        final_output = {
                            "extractionDetails": {
                                "extraction_id": extraction_id,
                                "document_id": document_id,
                                "annotation":"",
                                "output": {},
                                "error_message": status["message"],
                                "extraction_details": {
                                    "ext_extraction_name": extract_name,
                                    "ext_started_on": created_on,
                                },
                                "document_list": [
                                    {
                                        "dex_document_name": filename,
                                        "dex_created_on": created_on
                                    }
                                ]
                            }
                        }

                        final_outputs.append(copy.deepcopy(final_output))

                overall_status_code = 0 if success_count > 0 else 1
                message = SIMULATION_COMPLETED if success_count > 0 else SIMULATION_FAILED
            
                if success_count > 0:
                    overall_status_code = 0
                else:
                    overall_status_code = 1
                    message = SIMULATION_FAILED
            
                final_output = {
                    "simulation_id": simulation_id,
                    "output": final_outputs,
                    "annotation": annotation
                }
                logger.info(f"annotation is {annotation}")
                update_simulate_hist(data, final_outputs, simulation_id, "COMPLETED",annotation)
                success_res_obj = AgentResponse(
                    data=final_output,
                    status=StatusData(
                        code=overall_status_code,
                        message=message,
                    ),
                ).model_dump()
            
                yield from self._non_stream_response(success_res_obj)
            else:
                call_url = OrchConfig.URL.replace("mode", environ)
                logger.info(f"call url is {call_url}")
                logger.info(f"Request payload is {payload}")

                response = requests.post(call_url, headers=headers, json=payload, stream=True)
                response.raise_for_status()
                end = time.perf_counter()
                logger.info(f"Time taken to get response from orch: {end - start:.6f} sec.")
                logger.info(f"Response from orch {response.text}")
            if result["AD_ISCHAT_ENABLED"] == False:
                final_output, status, annotation = self.extract_final_output_from_stream(result=result,is_diet=True,extraction_id=extraction_id, 
                                                                          simulation_id=simulation_id,data=data,response=response)
                logger.info(f"final output from postprrcss when chat=false is {final_output}")
                if status["code"]==0:
                    if result["AD_AGENT_TYPE"] == "DOCUMENT_SEQA":
                        filename = data.input[0][result["AD_INTERFACE"]["Input"][0][0]["name"]][0]['file_name']
                        first_dict = data.input[0]
                        extract_name = list(first_dict.keys())[0]
                        extraction_id, document_id, created_on = update_extract_details(data,filename,simulation_id,extract_name)
                        final_output=self.diet_service(data,final_output,extraction_id,document_id,extract_name,created_on,filename,annotation)
                        update_extract_audit(data,final_output,extraction_id)
                        update_simulate_hist(data, final_output, simulation_id,'COMPLETED',annotation)
                    final_output = {"simulation_id": simulation_id, "output": final_output,"annotation":annotation}
                    logger.info(f"extraction_id is {final_output}")
        
                    success_res_obj = AgentResponse(
                        data=final_output,
                        status=StatusData(
                            code=0,
                            message=SIMULATION_COMPLETED,
                        ),
                    ).model_dump()
        
                    yield from self._non_stream_response(success_res_obj)
                else:
                    success_res_obj = AgentResponse(
                        data=final_output,
                        status=StatusData(
                            code=1,
                            message=status["message"],
                        ),
                    ).model_dump()
        
                    yield from self._non_stream_response(success_res_obj)
            else:
                if result["AD_AGENT_TYPE"] == "GENERIC" or (result["AD_AGENT_TYPE"] == "DOCUMENT_SEQA" and data.is_chat == True):
                    yield from self._stream_chat_response(response,simulation_id,user_message,data)
             
        except InvalidJson as e:
            yield from sse_error_response(data,simulation_id,INVALID_RESPONSE,code=1)
        except IncompleteConfigurationError as e:
            yield from sse_error_response(data,None,str(e),code=1)
        except InvalidUseCaseCode as e:
            yield from sse_error_response(data,None,e.message, code=0)
        except InvalidResponse as e:
            yield from sse_error_response(data,simulation_id,str(e),code=1)
        except InvalidAgentId as e:
            yield from sse_error_response(data,None,e.message, code=0)
        except ValidationError as e:
            yield from sse_error_response(data,None,str(e),code=1)
        except InvalidMode as e:
            yield from sse_error_response(data,simulation_id,str(e),code=1)
        except InvalidStatus as e:
            yield from sse_error_response(data,simulation_id,str(e),code=1)
        except InvalidInput as e:
            yield from sse_error_response(data,None,str(e),code=1)
        except FuncException as e:
            yield from sse_error_response(data,simulation_id,str(e),code=1)
        
        except Exception as e:
            logger.exception(f"Error in Simulating Agent Configuration {e}")
            yield from sse_error_response(data,None,"Simulation could not be completed due to a technical issue. Please try again later.",code=1)
 
    def _stream_chat_response(
        self, response, simulation_id, user_message, ip_data
    ) -> Generator[str, None, None]:
        """Generator for streaming workflow execution in SSE format."""
        try:
            collected_reasoning = ""
            for line in response.iter_lines():
                if not line:
                    continue
                line_str = line.decode("utf-8")
                if line_str.startswith("data: "):
                    data_str = line_str[6:]
                if data_str.strip() == "[DONE]":
                    break

                chunk = json.loads(data_str)
                
                reasoning=(chunk.get("data",{}).get("reasoning",""))
                if reasoning and reasoning.strip():
                    collected_reasoning += reasoning 
                chunk_type = chunk.get("type")
                yield self._format_sse_chunk(chunk)
                if chunk_type in "final":
                    data = chunk.get("data")
                    assistant_output = data["output"]
                    annotation=data["annotation"]
                    logger.info(f"annotation is {annotation}")
                    if "output" in assistant_output[0]:
                        assistant_output = assistant_output[0]['output']
                    final_reasoning=collected_reasoning.strip()
                    logger.info(f"assistant_output is {assistant_output}")
                    logger.info(f"Final reasoning: {final_reasoning}")
                    
                    self.chat_response(
                        ip_data, simulation_id, user_message, assistant_output,annotation,final_reasoning
                    )
                    yield YIELD_RESPONSE
                    return
                elif chunk_type in "error":
                    yield YIELD_RESPONSE
                    return

        except Exception as e:
            logger.exception("Error in streaming workflow execution")
            error_chunk = {"type": "error", "error": str(e), "data": []}
            yield self._format_sse_chunk(error_chunk)
            # Send [DONE] signal after error
            yield YIELD_RESPONSE
    
    def chat_simulate(self, data, result, input_data):
        global session_memory
        if not data.simulation_id:
            if result["AD_AGENT_TYPE"] == "GENERIC":
                simulation_id = create_simulate_hist(data,'INPROGRESS', input_data=None)
            else:
                simulation_id = create_simulate_hist(data,'INPROGRESS', input_data)
                payload = []
                for obj in (input_data or []):
                    if not isinstance(obj, dict) or not obj:
                        continue
                    key = next(iter(obj.keys()))
                    entries = obj.get(key, [])
                    if not isinstance(entries, list):
                        entries = [entries]
                    logger.info(f" data.input is {data.input}")


                    extract_name = list(data.input[0].keys())[0]
                    filename_list = data.input[0][extract_name]
                    
                    for entry, file_entry in zip(entries,filename_list):
                        single_input=[{extract_name:[file_entry]}]
                        filename=file_entry["file_name"]                                               
                        extraction_id, document_id, created_on = update_extract_details(
                            data, filename, simulation_id, extract_name
                        )
                        create_audit_details(data, extraction_id, single_input, "INPROGRESS")

                        user_message = {}  
                        payloads = create_payload(
                            data,
                            simulation_id,
                            result["AD_USECASE_CODE"],
                            result["AD_VERSION"],
                            single_input,
                            extraction_id
                        )

                        payload.append({
                            "payload": payloads,
                            "simulation_id": simulation_id,
                            "user_message": user_message,
                            "extraction_id": extraction_id,
                            "document_id": document_id,
                            "created_on": created_on,
                            "extract_name": extract_name,
                            "filename": filename,
                        })
                        logger.info(f"pppppppppppppppp {payloads}")

        else:
            simulation_id = data.simulation_id
        logger.info(f"chat simulation id is {simulation_id}")
        if simulation_id not in session_memory: 
            sim_history = get_simulation_hist_data(data, simulation_id) 
            if sim_history: 
                session_memory[simulation_id] = sim_history 
            else:
                session_memory[simulation_id] = [] 
        if result["AD_AGENT_TYPE"] == "GENERIC" or (result["AD_AGENT_TYPE"] == "DOCUMENT_SEQA" and data.is_chat==True):
            user_message = {
                "role": "user",
                "content": [{"type": "text", "text": input_data[0]["user_query"]}],
            }
            payload = create_chat_payload(
            data, simulation_id, result, user_message, session_memory[simulation_id]
        )
        logger.info(f"llllll {user_message}")  
        return payload, simulation_id, user_message

    def chat_response(self, data, simulation_id, user_message, assistant_output,annotation,final_reasoning):
        global session_memory
        if simulation_id not in session_memory:
            session_memory[simulation_id]=[]

        logger.info(f"assistant output {assistant_output}")
        bot_message = {
            "role": "assistant",
            "content": [{"type": "text", "text": assistant_output}],
        }
        new_output={
            "output":[user_message,bot_message],
            "reasoning":final_reasoning
        }
        session_memory[simulation_id].append(new_output)

        logger.info(f"session memory {session_memory[simulation_id]}")

        update_simulate_hist(data,session_memory[simulation_id], simulation_id,'COMPLETED',annotation)
        return bot_message

    def _non_stream_response(
        self, result: Dict[str, Any]
    ) -> Generator[str, None, None]:
        """
        Generator for non-streaming workflow execution in SSE format.
        Wraps result with standard structure: type, data, status.
        """
        try:
            # Wrap result with type field following standard structure
            if result["status"]["code"] == 0:
                # Success - send final chunk
                final_chunk = {
                    "type": "final",
                    "data": result["data"],
                    "status": result["status"],
                }
                yield self._format_sse_chunk(final_chunk)
            else:
                # Error - send error chunk
                error_chunk = {
                    "type": "error",
                    "data": result["data"],
                    "status": result["status"],
                }
                yield self._format_sse_chunk(error_chunk)

            # Send [DONE] signal
            yield YIELD_RESPONSE

        except Exception as e:
            logger.exception("Error in non-streaming workflow response")
            error_chunk = {
                "type": "error",
                "data": [],
                "status": {"code": 1, "message": str(e)},
            }
            yield self._format_sse_chunk(error_chunk)
            yield YIELD_RESPONSE


    def publish_config_service(self, data):
        try:
            logger.info("Inside publish service")
            validate_simulate_input(data)
            result,mcp_data = fetch_records_by_usecase_code_publish(data)
            orch_config = result["AD_ORCH_CONFIG"]
            orch_config = remove_coordinates(orch_config)
            aixp_data = CacheData(
                agent_id=result["AD_ID"],
                usecase_code=result["AD_USECASE_CODE"],
                version=result["AD_VERSION"],
                status=result["AD_STATUS"],
                orch_config=orch_config,
                system_prompt=result["AD_SYSTEM_PROMPT"],
                interface=result["AD_INTERFACE"],
                agent_config=result["AD_CONFIG"],
                is_chat_enabled=result["AD_ISCHAT_ENABLED"],
                agent_code=result["AD_AGENT_CODE"],
                agent_type=result["AD_AGENT_TYPE"],
                api_key=result["AD_API_KEY"],
                mcp_config=mcp_data
            )
            logger.info(f"aixp data is {aixp_data}")
            key = f"UC_{data.usecase_code}"
            chatbot_key=f"AGENT_PASSCODE_{aixp_data.agent_code}"
            logger.info(f"Caching agent passcode : {chatbot_key}")
            chatbot_agent_config = {
                "agent_id":aixp_data.agent_id,
                "agent_code":aixp_data.agent_code,
                "passcode":aixp_data.api_key
            }
            redis_client = get_redis_client("rte")
            set_cache(chatbot_key, chatbot_agent_config, redis_client)
            logger.info(f"Caching agent config for chatbot : {chatbot_agent_config}")

            payload = create_payload_publish(aixp_data, result["AD_USECASE_CODE"], key)
            logger.info(f"payload for publish is {payload}")
            response = pub_obj.post(payload)
            response = response.get_json()
            logger.info(f"response is {response}")
            if response["error"]["code"] != "200":
                error_message = response["error"]["message"]
                logger.info(f"error message is {error_message}")
                raise InvalidResponse(error_message)
            elif response["error"]["code"] == "200":
                activate_agent(data)
            update_publish_data(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=0,
                    message="Successfully published the agent configurations",
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidResponse as e:
            invalid_response_excp = invalid_response(
                data=FailedResponseData(), code=1, message=e.message
            )
            return invalid_response_excp
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except InvalidMode as e:
            mode_excp = invalid_mode(
                data=FailedResponseData(), code=1, message=e.message
            )
            return mode_excp
        except InvalidStatus as e:
            invalid_status_excp = invalid_status(
                data=FailedResponseData(), code=1, message=e.message
            )
            return invalid_status_excp
        except IncompleteConfigurationError as e:
            config_excp = configuration_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return config_excp
        except Exception as e:
            logger.exception(f"Error in Publishing Agent Configuration {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1, message="Error in Publishing Agent Configuration"
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def get_master_data_records_service(self, data):
        try:
            logger.info("Inside fetch master data")
            result = get_master_data_records(data)
            data_list = []
            for item in result:
                subset_data = [
                    SubData(**sub_item) for sub_item in item.get("subValues", [])
                ]
                master_data = MasterData(
                    code=item.get("code"),
                    value=item.get("value"),
                    defaultParameters=item.get("defaultParameters"),
                    desc=item.get("desc"),
                    subValues=subset_data,
                )
                data_list.append(master_data)
            response = AgentResponse(
                data=data_list,
                status=StatusData(
                    code=0,
                    message="Master Data",
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidModelType as e:
            mode_excp = invalid_mode(
                data=FailedResponseData(), code=1, message=e.message
            )
            return mode_excp
        except Exception as e:
            logger.exception(f"Error in getting Agent Configuration Details {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=1, message="Error in getting Agent Configuration Details"),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def get_published_records_by_agent_id_service(self, data):
        try:
            logger.info("In fetch published record based on agentid Service")
            result = get_published_records_by_agent_id(data)
            data_list = []
            for row in result:
                data = PublishData(
                    pd_id=row.get("PD_ID"),
                    pd_agent_id=row.get("PD_AD_ID"),
                    pd_version=row.get("PD_VERSION"),
                    pd_usecase_code=row.get("PD_USECASE_CODE"),
                    pd_updated_by=row.get("PD_UPDATED_BY"),
                    pd_updated_date=row.get("PD_UPDATED_DATE"),
                )
                data_list.append(data)
            response = AgentResponse(
                data=data_list,
                status=StatusData(
                    code=0, message="Published Data Details based on AgentID"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except Exception as e:
            logger.exception(
                f"Error in Displaying Published Data Details based on AgentId {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Published Data Details based on AgentId",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def fetch_tool_details_service(self, data):
        try:
            logger.info("In fetch tool records")
            result = fetch_tool_details(data)
            data_list = []
            for item in result:
                subset_data = [
                    SubCompData(**sub_item) for sub_item in item.get("sub_comp", [])
                ]
                config_tools = CompData(
                    component_group=item.get("comp_name"),
                    component_type=item.get("comp_type"),
                    display_name=item.get("display_name"),
                    properties=item.get("properties"),
                    sub_components=subset_data,
                )
                data_list.append(config_tools)
            response = AgentResponse(
                data=data_list, status=StatusData(code=0, message="Tool Details")
            )
            success_res_obj = make_response(
                json.dumps(response.model_dump(), sort_keys=False)
            )
            logger.info(f"response obj is {success_res_obj}")
            return success_res_obj
        except Exception as e:
            logger.exception(f"Error in Displaying Tool Details {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Tool Details",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def rte_chat_simulate(self, data, transaction_id, input_data,agent_type,agent_id):
        if agent_type == "GENERIC":
            user_message = {
                "role": "user",
                "content": [{"type": "text", "text": input_data[0]["user_query"]}],
            }
            payload = create_rte_chat_payload(
            data, transaction_id, user_message,agent_id
        )
        else:
            payload = []
            for obj in (input_data or []):
                if not isinstance(obj, dict) or not obj:
                    continue
                key = next(iter(obj.keys()))
                entries = obj.get(key, [])
                if not isinstance(entries, list):
                    entries = [entries]

                extract_name = list(data.input[0].keys())[0]
                filename_list = data.input[0][extract_name]
                for entry, file_entry in zip(entries,filename_list):
                    single_input=[{extract_name:[file_entry]}]
                    filename=file_entry["file_name"]                                               
                    extraction_id, document_id, created_on = update_rte_extract_details(
                        data, filename, transaction_id, extract_name
                    )

                    create_rte_audit_details(data, extraction_id, single_input, "INPROGRESS")

                    user_message = {}  
                    payloads = create_rte_payload(data, agent_id, single_input, transaction_id,extraction_id)
                    user_message = {}  
                    payload.append({
                        "payload": payloads,
                        "simulation_id": transaction_id,
                        "user_message": user_message,
                        "extraction_id": extraction_id,
                        "document_id": document_id,
                        "created_on": created_on,
                        "extract_name": extract_name,
                        "filename": filename,
                    })
        
        return payload, transaction_id, user_message

    def rte_simulate_config_service(self, bearer_token, data):
        return Response(
        stream_with_context(
            self._rte_simulate_generator(bearer_token, data)
        ),
        headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

    def _rte_simulate_generator(self, bearer_token, data):
        try:
            logger.info("Inside rte simulate service")
            headers = {"Authorization": f"{bearer_token}"}
            redis_client = get_redis_client(data.type)
            key = f"UC_{data.usecase_code}"
            interface_schema,agent_id,_,agent_type,_,is_chat_enabled,_ = get_cache(key, redis_client)
            input_schema = interface_schema["Input"]
            input_data = validate_input(data.input, input_schema)
            if not data.transaction_id:
                transaction_id = generate_transaction_id(data)
            else:
                logger.info('entered in else')
                transaction_id = data.transaction_id
            
            logger.info(f"Transaction id is {transaction_id}")
            is_chat_enabled = str(is_chat_enabled).strip().lower() == "true"
            if is_chat_enabled:
                payload, transaction_id, user_message = self.rte_chat_simulate(data, transaction_id, input_data,agent_type,agent_id)
            else:
                payload = create_rte_payload(data, agent_id, input_data, transaction_id)

            logger.info(f"Request payload is {payload}")
            init_chunk = {
                    "type": "initiate",
                    "data": {"simulation_id": transaction_id,
                        "output": "Simulation initiated successfully"
                    }
                }
            yield self._format_sse_chunk(init_chunk)
            environ = data.type

            if is_chat_enabled and agent_type=="DOCUMENT_SEQA" and data.is_chat == False:
                entries = payload
                if not isinstance(entries, list):
                    entries = [{"payload": payload, "user_message": user_message}]
                
                final_outputs = []
                message = SIMULATION_COMPLETED
            
                success_count = 0

                for idx, entry in enumerate(entries, start=1):
                    per_payload   = entry.get("payload")
                    user_message  = entry.get("user_message", {})
                    extraction_id = entry.get("extraction_id")
                    document_id   = entry.get("document_id")
                    created_on    = entry.get("created_on")
                    extract_name  = entry.get("extract_name")
                    filename      = per_payload["input"][0][list(per_payload["input"][0].keys())[0]][0]["file_name"]

                    logger.info(f"Payload for orch is {per_payload}")
                    call_url = OrchConfig.URL.replace("mode", environ)

                    logger.info(f"call_url is {call_url}")
                    response = requests.post(call_url, headers=headers, json=per_payload, stream=True)
                    logger.info(f"Response from orch {response.text}")
                    response.raise_for_status()

                    final_output, status, annotation = self.extract_final_output_from_stream(is_diet=False,result=interface_schema,data=data,
                                                                                                response=response,extraction_id=extraction_id)

                    logger.info(f"final output from postprocess is {final_output}\n, "
                        f"status is {status}, annotation is {annotation}")

                    if status["code"] == 0:
                        success_count += 1      
                        final_output = self.diet_service( data,final_output, extraction_id, document_id,extract_name,created_on,filename,annotation)
                        final_outputs.append(copy.deepcopy(final_output))
                    else:
                        logger.info(f'Orchestrator returned error code: {status["code"]}')

                        final_output = {
                            "extractionDetails": {
                                "extraction_id": extraction_id,
                                "document_id": document_id,
                                "annotation":"",
                                "output": {},
                                "error_message": status["message"],
                                "extraction_details": {
                                    "ext_extraction_name": extract_name,
                                    "ext_started_on": created_on,
                                },
                                "document_list": [
                                    {
                                        "dex_document_name": filename,
                                        "dex_created_on": created_on
                                    }
                                ]
                            }
                        }

                        final_outputs.append(copy.deepcopy(final_output))

                overall_status_code = 0 if success_count > 0 else 1
                message = SIMULATION_COMPLETED if success_count > 0 else "Simulation Failed"
            
                if success_count > 0:
                    overall_status_code = 0
                else:
                    overall_status_code = 1
                    message = "Simulation Failed"
            
                final_output = {
                    "transaction_id": transaction_id,
                    "output": final_outputs,
                    "annotation": annotation
                }
            
                success_res_obj = AgentResponse(
                    data=final_output,
                    status=StatusData(
                        code=overall_status_code,
                        message=message,
                    ),
                ).model_dump()
            
                yield from self._non_stream_response(success_res_obj)
            else:

                call_url = OrchConfig.URL.replace("mode", environ)
                response = requests.post(call_url, headers=headers, json=payload)
                response.raise_for_status()
                logger.info(f"Response from rte orch {response.text}")
            
            
            if not is_chat_enabled:
                if agent_type == "DOCUMENT_SEQA":
                    filename = data.input[0][input_schema["Input"][0][0]["name"]][0]['file_name']
                    first_dict = data.input[0]
                    extract_name = list(first_dict.keys())[0]
                    extraction_id, document_id, created_on = update_rte_extract_details(data,filename,transaction_id,extract_name)
                    if data.bancs==True:
                        final_output,status,annotation = self.extract_final_output_from_stream(is_diet=False,result=interface_schema,data=data,
                                                                            response=response
                                                                            )
                        logger.info(f"bancs_recommend-rte-doc_seqa final output from post prcss when chat=false is {final_output}")
                        if status["code"]==0:
                            message = SIMULATION_COMPLETED
                        else:
                            message = status["message"]
                    else:
                        final_output,status,annotation = self.extract_final_output_from_stream(is_diet=False,result=interface_schema,data=data,
                                                                            response=response
                                                                            )
                        logger.info(f"rte-doc_seqa final output from post prcss when chat=false is {final_output}")
                        if status["code"]==0:
                            final_output=self.diet_service(data,final_output,extraction_id,document_id,extract_name,created_on,filename,annotation)
                            logger.info(f"rte-doc_seqa final output from post prcss when chat=false is {final_output}")
                            message=SIMULATION_COMPLETED
                        else:
                            message=status["message"]
                elif agent_type == "GENERIC" or data.bancs==True:
                    final_output,status,annotation = self.extract_final_output_from_stream(is_diet=False,result=interface_schema,data=data,
                                                                            response=response
                                                                            )
                    logger.info(f"rte-generic final output from post prcss when chat=false is {final_output}")
                    if status["code"]==0:
                        message = SIMULATION_COMPLETED
                    else:
                        message = status["message"]
                final_output = {"simulation_id": transaction_id, "output": final_output, "annotation":annotation}
                logger.info(f"final_output is {final_output}")
                success_res_obj = AgentResponse(
                    data=final_output,
                    status=StatusData(
                        code=0,
                        message=message,
                    ),
                ).model_dump()
    
                yield from self._non_stream_response(success_res_obj)
            else:
                if agent_type == "GENERIC":
                    yield from self._stream_chat_response(response,transaction_id,user_message,data)
                
        except InvalidJson as e:
            yield from sse_error_response(data,transaction_id,INVALID_RESPONSE,code=1)
        except IncompleteConfigurationError as e:
            yield from sse_error_response(data,None,str(e),code=1)
        except InvalidUseCaseCode as e:
            yield from sse_error_response(data,None,e.message, code=0)
        except InvalidResponse as e:
            yield from sse_error_response(data,transaction_id,str(e),code=1)
        except InvalidAgentId as e:
            yield from sse_error_response(data,None,e.message, code=0)
        except ValidationError as e:
            yield from sse_error_response(data,transaction_id,str(e),code=1)
        except InvalidMode as e:
            yield from sse_error_response(data,transaction_id,e.message,code=1)
        except FuncException as e:
            yield from sse_error_response(data,transaction_id,str(e),code=1)
        except InvalidStatus as e:
            yield from sse_error_response(data,transaction_id,str(e),code=1)
        except InvalidInput as e:
            yield from sse_error_response(data,None,str(e),code=1)
        except Exception as e:
            logger.exception(f"Error in Simulating Agent Configuration {e}")
            yield from sse_error_response(data,transaction_id,"Simulation could not be completed due to a technical issue. Please try again later.",code=1)

    def _format_sse_chunk(self, chunk: Dict[str, Any]) -> str:
        """Format chunk as SSE (Server-Sent Events) data."""
        return f"data: {json.dumps(chunk)}\n\n"
    
    def update_agent_desc_service(self, data):
        try:
            logger.info("In Update Agent Details Service")
            update_agent_details(data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=0, message="Successfully updated Agent Details"),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except MissingFields as e:
            missing_field_excp = missing_field_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return missing_field_excp
        except Exception as e:
            logger.exception(f"Error in Updating Agent Details() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=1, message="Error in Updating Agent Details"),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def retrieve_log_details_service(self, data, bearer_token):
        try:
            logger.info("Inside Fetch Log Service")
            headers = {"Authorization": f"{bearer_token}"}
            payload = create_payload_log(data)
            logger.info(f"payload for log {payload}")
            environ = data.type
            call_url = OrchLogConfig.URL_LOG.replace("mode", environ)
            logger.info(f"payload for retrieve logs {payload}")
            response = requests.post(call_url, headers=headers, json=payload)
            logger.info(f"response from orch for log {response.text}")
            if response.status_code == 200:
                response = response.json()
                if response["status"]["code"] == 0:
                    response = response["data"]
                    logger.info(f"Logs from orch {response}")
                elif response["status"]["code"] == 1:
                    error_message = response["status"]["message"]
                    raise InvalidResponse(error_message)
            else:
                logger.info(f"response is {response.status_code}")
                logger.info(f"response data is {response.text}")
                raise InvalidJson

            response = AgentResponse(
                data=response,
                status=StatusData(
                    code=0,
                    message="Log Details",
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidResponse as e:
            invalid_response_excp = invalid_response(
                data=FailedResponseData(), code=1, message=e.message
            )
            return invalid_response_excp
        except InvalidJson as e:
            invalid_json_excp = invalid_json_exception(
                data=FailedResponseData(), code=1, message=INVALID_RESPONSE
            )
            return invalid_json_excp
        except Exception as e:
            logger.exception(f"Error in Displaying Log Details() {e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(code=1, message="Error in Displaying Log Details"),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def retrieve_simulate_hist_service(self, data):
        try:
            logger.info("In Fetch Simulate Hist based on agentid Service")
            result = get_simulate_hist_records_by_agent_id(
                data
            )
            data_list = []
            if len(result)!=0:
                for row in result:
                    updated_date=row.get("SH_UPDATED_DATE")
                    if updated_date:
                        updated_date=updated_date.strftime("%d-%m-%Y %H:%M:%S")

                    data = SimulationHistData(
                        sh_id=row.get("SH_ID"),
                        sh_sim_id=row.get("SH_SIM_ID"),
                        sh_ad_id=row.get("SH_AD_ID"),
                        sh_status=row.get("SH_STATUS"),
                        sh_created_by=row.get("SH_CREATED_BY"),
                        sh_created_date=row.get("SH_CREATED_DATE").strftime("%d-%m-%Y %H:%M:%S"),
                        sh_updated_by=row.get("SH_UPDATED_BY"),
                        sh_updated_date=updated_date,
                    )
                    data_list.append(data)
            else:
                return []
            response = AgentResponse(
                data=data_list,
                status=StatusData(
                    code=0, message="Simulation Hist Data based on AgentID"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except Exception as e:
            logger.exception(
                f"Error in Displaying Simulation Hist Data based on AgentId {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Simulation Hist Data based on AgentId",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj
        
    def retrieve_simulate_details(self, data):
        try:
            logger.info("In Fetch Simulate details based on Simulation id Service")
            input_fields, output_fields,error_message,annotation,chat_fields = get_simulate_details_by_sim_id(
                data
            )
            data = SimulationDetails(
                    sh_chat =chat_fields,
                    sh_input=input_fields,
                    sh_output=output_fields,
                    sh_error_message=error_message,
                    sh_annotation=annotation
                )
            response = AgentResponse(
                data=data,
                status=StatusData(
                    code=0, message="Simulation details fetched successfully based on Simulation"
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidSimulationId as e:
            sim_id_excp = simulation_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return sim_id_excp
        except InvalidExtractionId as e:
            ext_id_excp = extraction_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return ext_id_excp
        except Exception as e:
            logger.exception(
                f"Error in Displaying Simulation Details based on Simulation Id {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Simulation Details based on Simulation Id",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    def get_rte_details(self, data):
        try:
            logger.info("Inside Rte details")
            redis_client = get_redis_client(data.type)
            key = f"UC_{data.usecase_code}"
            interface_schema,agent_id,agent_version,agent_type,agent_config,is_chat_enabled,api_key = get_cache(key, redis_client)

            data = CacheConfigData(
                agent_id=agent_id,
                agent_type=agent_type,
                interface=interface_schema,
                agent_version=agent_version,
                agent_config=agent_config,
                is_chat_enabled=is_chat_enabled,
                api_key=api_key
            )
            response = AgentResponse(
                data=data,
                status=StatusData(
                    code=0,
                    message="Agent Details",
                ),
            )
            success_res_obj = make_response(response.model_dump(mode="json"))
            return success_res_obj
        except NoDataException as e:
            no_data_excp = no_data_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return no_data_excp
        except Exception as e:
            logger.exception(f"Error in Displaying Agent Details{e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Displaying Agent Details",
                ),
            )
            failure_res_obj = make_response(response.model_dump(mode="json"))
            return failure_res_obj

    def retrieve_pdf_preview(self,data):
        try:
            file_name,usecase_code = retrieve_filename(data)
            logger.info(f"file_name is {file_name[0]}")
            if data.type=='cre':
                base_path=os.getenv("CRE_REPO_FILE_PATH")
            elif data.type=='rte':
                base_path=os.getenv("RTE_REPO_FILE_PATH")
            
            file_path = Path(base_path) / str(usecase_code) / str(file_name)
            file_path = file_path.as_posix()
            logger.info(f'file_path is {file_path}')
            if not file_path or not os.path.isfile(file_path):
                raise FileNotFound
            with open(file_path, "rb") as f:
                encoded_content  = base64.b64encode(f.read()).decode("utf-8")
            file_data = FileData(
                filename="file.pdf",
                content_type="application/pdf",
                file_base64=encoded_content
            )

            response = AgentResponse(
                    data=file_data,
                    status=StatusData(
                        code=0,
                        message="File fetched successfully",
                    ),
                )
            
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidExtractID as e:
            logger.exception(f"Issue in retrieve_pdf_preview: {e}")
            response = ServiceResponse(
                data=FailedResponseData(),
                status=sd(
                    code=1,
                    message=e.message,
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj  
        except Exception as e:
            logger.exception(f"Error in Fetching Files{e}")
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message=str(e),
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj
        
    def json_safe(self,obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return str(obj) 


    def export_agent_details_to_json(self, data):
        try:
            logger.info("In export json based on agentid Service")
            agent_details = get_records_by_agent_id(data)
            logger.info(f"agent details is {agent_details}")
            file_group=get_groups_by_agent_id(data)
            logger.info(f"file group is {file_group}")
            file_details=fetch_file_content_by_agent_id(data)
            logger.info(f"file data is {file_details}")
            _,server_details,mcp_tool_details=retrieve_mcp_server_details(data)
            logger.info(f"server_details is {server_details}")
            logger.info(f"mcp_tool_details is {mcp_tool_details}")
            
            zip_buffer = io.BytesIO()


            with zipfile.ZipFile(
                zip_buffer,
                mode="w",
                compression=zipfile.ZIP_DEFLATED
            ) as zf:

                zf.writestr(
                    "agent_details.json",
                    json.dumps(agent_details, indent=4,default=self.json_safe)
                )
    
                # Context management
                zf.writestr(
                    "context_management/file_group.json",
                    json.dumps(file_group, indent=4,default=self.json_safe)
                )
                zf.writestr(
                    "context_management/file_details.json",
                    json.dumps(file_details, indent=4,default=self.json_safe)
                )
    
                # MCP tools
                zf.writestr(
                    "mcp_tools/server_details.json",
                    json.dumps(server_details, indent=4,default=self.json_safe)
                )
                zf.writestr(
                    "mcp_tools/mcp_tool_details.json",
                    json.dumps(mcp_tool_details, indent=4,default=self.json_safe)
                )
    
            zip_buffer.seek(0)
            return send_file(
                zip_buffer,
                mimetype="application/zip",
                as_attachment=True,
                download_name="AGENTS.zip"
            )

        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except Exception as e:
            logger.exception(
                f"Error in Exporting Agent Details based on AgentId {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Exporting Agent Details based on AgentId",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj

    
    def import_agent_details_to_json(self,file, data):
        try:
            logger.info("In import json  based on agentid Service")
            logger.info(f"file is {file}")
            validate_zip_file(file)
            logger.info(f"validate zip {file}")
            extract_files=extract_json_files_from_zip(file)
            logger.info(f"extracted file is {extract_files}")
            result=import_agent_details(extract_files,data)
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=result['code'], message=result['message']
                ),
            )
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except FileNotFoundException as e:
            file_not_found_excp=file_not_found(data=FailedResponseData(), code=0, message=e.message)
            return file_not_found_excp
        except InvalidFileException as e:
            invalid_file_excp= invalid_file_exception(data=FailedResponseData(), code=0, message=e.message)
            return invalid_file_excp
        except UseCaseCodeException as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except InvalidUseCaseCode as e:
            usecase_code_excp = usecase_code_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return usecase_code_excp
        except  UseCaseNameMissMatch as e:
            usecase_code_excp = usecase_name_exception(
                data=FailedResponseData(), code=1, message=e.message
            )
            return usecase_code_excp
        except Exception as e:
            logger.exception(
                f"Error in Importing Agents {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Importing Agents",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj
       

    def retrieve_list_of_availabe_files(self, data):
        try:
            logger.info("In export json based on agentid Service")
            result = get_available_files_by_agent_id(data)          
            response = AgentResponse(
                    data=result,
                    status=StatusData(
                        code=0,
                        message="File fetched successfully",
                    ),
                )            
            success_res_obj = make_response(response.model_dump())
            return success_res_obj
        except InvalidAgentId as e:
            agent_id_excp = agent_id_exception(
                data=FailedResponseData(), code=0, message=e.message
            )
            return agent_id_excp
        except Exception as e:
            logger.exception(
                f"Error in Retrieving available files {e}"
            )
            response = AgentResponse(
                data=FailedResponseData(),
                status=StatusData(
                    code=1,
                    message="Error in Retrieving available files",
                ),
            )
            failure_res_obj = make_response(response.model_dump())
            return failure_res_obj
