import os
import sys
import subprocess
import logging
import re
import base64

# Log entry format
log_entry_format = '%(module)s %(asctime)s [%(levelname)s] %(message)s'
# Logger initialization
logging.basicConfig(
  filename='/home/nutanix/data/logs/xiint-tenant-config-push.out',
  level=logging.INFO,
  format=log_entry_format)

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
formatter = logging.Formatter(log_entry_format)
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

log = logging.getLogger(__name__)

if os.path.isdir("/usr/local/nutanix"):
    sys.path.insert(0, "/usr/local/nutanix")
    sys.path.insert(0, "/usr/local/nutanix/bin")
os.environ["ZOOKEEPER_HOST_PORT_LIST"] = "zk1:9876"

try:
    import env
except ImportError as ex:
    log.error("Error during importing env:" + ex)
    pass

import requests
import time
import json
import requests
try:
  import cluster.client.consts as consts
except ImportError:
  import cluster.consts as consts
import cluster.genesis_utils as genesis_utils

from util.net.rpc import RpcError
from util.misc.retry import retry_with_exp_backoff, retry_with_delay

import util.zookeeper.zookeeper_interface as zookeeper_interface
from aplos.sl_bufs.iam_service_account_config_pb2 import \
  IamServiceAccountConfig
from util.misc.protobuf import pb2json
from util.iam.token_client import TokenClient
from aplos.sl_bufs.xi_mgmt_config_pb2 import XiPcConfig
from util.interfaces.interfaces import NutanixInterfaces, NutanixInterfacesError


VAULT_KV1_SECRET_PATH = "v1/secret/data"
VAULT_KV2_SECRET_PATH = "v1/secret/data/data"
VAULT_IAM_AUTH_PATH = "v1/auth/iam/login"

def get_lib():
  from cluster.genesis.node_manager import NodeManager
  from cluster.utils import genesis_client
  return NodeManager, genesis_client

def get_params(genesis_utils, start):
  method = "services_start" if start else "services_stop"
  param_key = "services_to_start" if start else "services_to_stop"
  return method, param_key, genesis_utils.get_svm_ips()


def service_lifecycle(start=True, services_list=[], cvm_ips=None):
  NodeManager, genesis_client = get_lib()
  method, param_key, svm_ips = get_params(genesis_utils, start)
  if cvm_ips is not None:
    svm_ips = cvm_ips

  op = "start" if start else "stop"
  oped = 'started' if start else "stopped"

  genesis_api_client = genesis_client.GenesisApiClient()
  ret_dict = genesis_api_client.make_rpc_on_svms(
    "NodeManager",
    method,
    {param_key: services_list},
    svm_ips=svm_ips)

  result = True
  for ip, ret in ret_dict.iteritems():
    if not ret:
      result = False
      log.error("Operation failed at svm {0}".format(ip))
      break

  if result:
    log.info("Successfully {0} {1}".format(oped, services_list))
  else:
    log.error("Failed to {0} {0}".format(op, services_list))

  return result

# Function to run a cmd string on all nodes in the cluster
def run_cmd_on_all_cvms(svmips, cmd):
    """blocking cmd execution that returns stdout and stderr
    Args:
        cmd (string): command to execute across all nodes in a cluster
        return's False when execution in any node fails
        returns status Dict containing (ip, ret, err)
    """
    cmd_map = dict([(x, cmd) for x in svmips])
    status = genesis_utils.run_command_on_svms(cmd_map)
    for ip, (ret, out, err) in status.items():
        if ret != 0:
          log.error("Command: {0} failed for svm ip: {1} with output: {2} and error: {3}".format(cmd, ip, out, err))
          return False
        log.info("Command: {0} ran successfully for svm ip: {1}. Output not printed for security reasons".format(cmd, ip))
    return status


def run_cmd_on_all_cvms_in_sequence(svmips, cmd):
    """blocking cmd execution that returns stdout and stderr. This runs command in sequence on all CVMs
    Args:
        cmd (string): command to execute across all nodes in a cluster
        return's False when execution in any node fails
        returns status Dict containing (ip, ret, err)
    """
    for ip in svmips:
      cmd_map = dict([(ip, cmd)])
      status = genesis_utils.run_command_on_svms(cmd_map)
      log.info("STATUS: %r" %status)
      for ip, (ret, out, _) in status.items():
        if ret != 0:
            return False
        else:
            out = re.sub(r"[\n\t\s]*", "", out)
            log.info("ip: %r OUT - %r" %(ip, out))
    return True


# To run a shell cmd and return output and error back
def run_cmd(cmd, silent_mode=True, output_fd=subprocess.PIPE):
    """blocking cmd execution that returns stdout and stderr
    Args:
        cmd (string): command to execute
        output_fd (file): existing open file to write output to
        silent_mode(bool): decides whether output is printed to screen
    """
    log.debug("Starting to run command: %s", cmd)

    # Execute command in new process and return Popen object
    p = subprocess.Popen(cmd,
                         shell=True,
                         stdin=subprocess.PIPE,
                         stdout=output_fd,
                         stderr=subprocess.STDOUT)
    # block popen to get output and error if any
    out, err = p.communicate()
    if not err:
        if not silent_mode:
            if out:
                log.info("Command output is:\n"+out);
        log.debug("Success running command: %s", cmd)
    else:
        log.info("Error running command: %s", cmd)
        log.info("Error is\n:"+err)
    return out, err


# To execute a shell command and then continuously buffer output to stdout
def execute(cmd):
    """
    Simple execute function to execute cmd in shell
    :param cmd:
    :return: returns process return code
    """
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

"""
Overwrites the specified gflag in the config file with new values.
"""
def append_to_gflags(svm_ips, gflags_file, gflag_name, gflag_value):
  temp_file = gflags_file + ".bak"

  try:
    if os.path.exists(temp_file):
      os.remove(temp_file)

    if os.path.exists(gflags_file):
      with open(gflags_file) as old_file:
        with open(temp_file, "w+") as new_file:
          for flag in old_file:
            if not flag.startswith('--%s' % gflag_name):
              new_file.write(flag)
      os.remove(gflags_file)

    with open(temp_file, "a+") as new_file:
      new_file.write("--%s=%s\n" % (gflag_name, gflag_value))

    os.rename(temp_file, gflags_file)
  except Exception as e:
    utils.log.error("Exception while rewriting %s : %s " % (gflags_file, str(e)))
    sys.exit(1)

  for ip in svm_ips:
    cmd = 'scp %s nutanix@%s:%s' % (gflags_file, ip, gflags_file)
    ret, err = run_cmd(cmd)
    if err:
      utils.log.error("Failed to update gflags on svm %s : %s" % (ip, err))
      sys.exit(1)


# restart services
def restart_services(services):
  svmips = genesis_utils.get_svm_ips()
  for svm in svmips:
    log.info("Restarting the services:%s on svm: %s" % (services, svm))
    if not service_lifecycle(start=False, services_list=services, cvm_ips=[svm]):
      log.error("Failed to stop services: %s on SVM:%s" % (services, svm))
    if not service_lifecycle(start=True, services_list=services, cvm_ips=[svm]):
      log.error("Failed to start services: %s on SVM:%s" % (services, svm))
      sys.exit(1)

# Secret Store Util
class SecretStoreHelperException(Exception):
  pass


class SecretNotFoundException(Exception):
  pass


class SecretStoreHelper(object):
  RETRY_SLOT_TIME_MS = 1000
  RETRY_MAX_DELAY_TIME_MS = 5000
  RETRY_MAX_COUNT = 5

  VAULT_KV_SECRET_PATH = "v1/secret/data"
  VAULT_IAM_AUTH_PATH = "v1/auth/iam/login"

  VAULT_BASE_URL = "https://{address}:{port}/"

  VALID_SECRET_TYPES = ["user+pass", "user+key", "ssl+private", "ssl+public",
                        "string"]
  VALID_ENCODING_LIST = ["base64"]

  VAULT_SECRET_DATA_KEY = "data"
  VAULT_SECRET_ENCODING_KEY = "encoding"
  VAULT_SECRET_TYPE_KEY = "type"
  VAULT_SECRET_USABILITY_KEY = "_usability"

  def __init__(self, address, iam_token, port=443, verify_session=None):
    """
    Args
      adress(str): Address for secret store. (FQDN or IP)
      port(str): Port for Secret store.
      iam_token(str): IAM token to use for requests
      verify_session(str): Path to the CA chain. Default None.
    """
    self._secret_base = self.VAULT_BASE_URL.format(address=address, port=port)
    self._iam_token = iam_token
    self._verify_session = verify_session or False
    self._vault_token = None

  def get(self, path):
    """
    Retrieve the secret
    Args:
      path(str): Path to the secret
    Returns:
      dict, None: Dict with keys - "data", "encoding", "type" - on successfully
        retrieving secrets
      None, str: On error retrieving secrets.
    """
    method = "GET"
    url = "{0}{1}{2}".format(
      self._secret_base, VAULT_KV1_SECRET_PATH, path)
    headers, error = self._get_secret_store_headers()
    if error:
      log.error(error)
      return None, error

    log.debug("Sending request, method %s url %s headers %s" % (
      method, url, headers))
    resp, error = self._make_remote_call(method, url, headers)
    if error:
      # network error has occurred while making request
      log.error(error)
      return False, error

    # On error, try for key-value version 2
    if 400 <= resp.status_code <= 409:
      log.debug("Trying to get kv2 version of secret")
      v2_url = "{0}{1}{2}".format(self._secret_base, VAULT_KV2_SECRET_PATH,
                                       path)
      log.debug("Sending request, method: %s url: %s headers: %s" % (
        method, v2_url, headers))
      v2_resp, v2_error = self._make_remote_call(method, v2_url, headers)
      if v2_error:
        # network error has occurred while making request
        log.error(v2_error)
        return None, v2_error
      if not v2_resp or v2_resp.status_code != 200:
          # log kv v2 path as well
          log.error("Key-value v2: Failed to retrieve secret at '%s'. %s" %
                    (v2_url, v2_resp))
          # Always return kv v1 errors to maintain compatibility
          log.error("Failed to retrieve secret at '%s'. %s" % (path, resp))
          return None, resp.status_code
      # Doc: https://x/ss-kv-migration
      # Transform response to kv v1 response format
      # Current GET response format
      """
      {
          "data": {
              "data": "tLS0tCg==",
              "encoding": "base64",
              "type": "ssl+public"
          }
      }
      """
      # kv v2 GET response format
      """
      {
          "data": {
              "data": {
                  "data":   "tLS0tCg==",
                  "encoding": "base64",
                  "type": "ssl+public"
              },
              "metadata": {
                  "created_time": "2019-05-29T23:28:04.979206252Z",
                  "deletion_time": "",
                  "destroyed": false,
                  "version": 1
              }
          }
      }
      """
      v2_response_dict = v2_resp.json()
      v2_secret_data = v2_response_dict["data"]["data"]
      v2_result = {SecretStoreHelper.VAULT_SECRET_TYPE_KEY: None,
                SecretStoreHelper.VAULT_SECRET_ENCODING_KEY: None,
                SecretStoreHelper.VAULT_SECRET_DATA_KEY: None}
      v2_result.update(v2_secret_data)
      return v2_result, None

    if not resp or resp.status_code != 200:
      log.error("Failed to retrieve secret at '%s'. %s" % (path, resp))
      return None, resp.status_code

    response_dict = resp.json()
    if "data" not in response_dict:
      error = (
        "Response doesn't contain 'data' key. Response: %s" % response_dict)
      log.error(error)
      return None, error

    secret_data = response_dict["data"]
    result = {SecretStoreHelper.VAULT_SECRET_TYPE_KEY: None,
              SecretStoreHelper.VAULT_SECRET_ENCODING_KEY: None,
              SecretStoreHelper.VAULT_SECRET_DATA_KEY: None}
    result.update(secret_data)
    return result, None

  def update(self, path, secret, type, encoding, usability=None):
    """
    The secret encoded - with one of the supported encoding formats.
    Args:
      path(str): Path to the secret
      secret(encoded or non-encoded string): Actual secret data
      type(str): One of the supported secret types in VALID_SECRET_TYPES
      encoding(str): One of the encoding types in VALID_ENCODING_LIST
      usability(str): Optional. Comma Separated Values of the usability of the
                      secret.
                      Examples:
                        "usability": "internet,corpnet,dc"
                        "usability": "corpnet,dc"
                        "usability": "dc"
    Return:
      dict, None: Dict with keys - "data", "encoding", "type" - on successfully
        saving secrets
      None, str: On error retrieving secrets.
    Raises:
      SecretStoreHelperException(): Invalid type or encoding
    """
    if type not in SecretStoreHelper.VALID_SECRET_TYPES:
      raise SecretStoreHelperException(
        "Secret type: %s not in valid types: %s" % (
          type, SecretStoreHelper.VALID_SECRET_TYPES))
    if encoding and encoding not in SecretStoreHelper.VALID_ENCODING_LIST:
      raise SecretStoreHelperException(
        "Secret encoding: %s not in valid encoding: %s" % (
          encoding, SecretStoreHelper.VALID_ENCODING_LIST))
    method = "POST"
    url = "{0}{1}{2}".format(
      self._secret_base, VAULT_KV1_SECRET_PATH, path)
    data = {SecretStoreHelper.VAULT_SECRET_DATA_KEY: secret,
            SecretStoreHelper.VAULT_SECRET_ENCODING_KEY: encoding,
            SecretStoreHelper.VAULT_SECRET_TYPE_KEY: type}
    if usability is not None:
      data.update({SecretStoreHelper.VAULT_SECRET_USABILITY_KEY: usability})

    headers, error = self._get_secret_store_headers()
    if error:
      log.error(error)
      return False, error

    log.debug("Sending request, method: %s url: %s headers: %s" % (
      method, url, headers))
    resp, error = self._make_remote_call(method, url, headers, data=data)
    if error:
      # network error has occurred while making request
      log.error(error)
      return False, error

    # On error, try for key-value version 2
    if 400 <= resp.status_code <= 409:
      log.debug("Trying to update kv2 version of secret")
      v2_url = "{0}{1}{2}".format(self._secret_base, VAULT_KV2_SECRET_PATH,
                                       path)
      v2_data = {"data": data}
      log.debug("Sending request, method: %s url: %s headers: %s" % (
        method, v2_url, headers))
      v2_resp, v2_err = self._make_remote_call(method, v2_url, headers,
                                                data=v2_data)
      if v2_err:
        # network error has occurred while making request
        log.error(v2_err)
        return False, v2_err
      if v2_resp.status_code != 200:
          # log kv v2 path as well
          log.error("Key-value v2: Failed to create secret at '%s'. %s" %
                    (v2_url, v2_err))
          # Always return kv v1 errors to maintain compatibility
          log.error(resp)
          return False, resp.status_code
      return True, None

    # Secret Store key-value v1 returns 204 No Content on success
    # The server successfully processed the request and
    # is not returning any content.
    if resp.status_code != 204:
      log.error(resp)
      return False, resp.status_code

    return True, None

  def _get_secret_store_headers(self):
    """
    Get the headers for Vault after exchanging the IAM token for vault token.
    Returns:
      dict, str: Dict with keys 'content-type' and 'X-Vault-Token', None
        None, error message on failure to get vault token.
    """
    error = None
    if not self._vault_token:
      self._vault_token, error = self._get_vault_access_token()

    if self._vault_token:
      return {
               "content-type": "application/json",
               "X-Vault-Token": self._vault_token}, error
    else:
      return None, error

  def _get_vault_access_token(self):
    method = "PUT"
    url = "{0}{1}".format(self._secret_base, VAULT_IAM_AUTH_PATH)
    headers = {"Content-Type": "application/json"}
    data = {"token": self._iam_token}

    log.debug("Sending request, method %s url %s" % (method, url))
    resp, error = self._make_remote_call(method, url, headers, data=data)
    if error:
      log.error(error)
      return None, error

    if resp.status_code != 200:
      return None, resp.status_code

    resp_json = resp.json()
    log.debug("vault token: %s" % resp_json["auth"]["client_token"])

    return resp_json["auth"]["client_token"], error

  def _make_remote_call(self, method, url, headers,
                        data=None, timeout=20):
    if data is not None:
      data = json.dumps(data)
    err_str = "Unexpected error making request to Secret Store"
    resp = None
    for retry_num in retry_with_exp_backoff(
      self.RETRY_SLOT_TIME_MS, self.RETRY_MAX_DELAY_TIME_MS,
      max_retries=self.RETRY_MAX_COUNT):
      try:
        log.info("Making request to Secret Store. Try count: %d" % retry_num)
        resp = requests.request(
          method, url,
          headers=headers,
          data=data, allow_redirects=False, verify=self._verify_session,
          timeout=timeout)

        if resp is None or resp.status_code >= 500:
          log.debug(
            "Request failed:\nURL: %s\nmethod: %s\ndata: %s\nresp: %s\n"
            "Retrying request..." % (url, method, data, resp))
          continue
        return resp, None
      except requests.exceptions.RequestException as ex:
        err_str = "Failed to make request to Secret Store. {0}".format(ex)
        log.error(err_str)
    log.error("Request failed.\nURL: %s\nmethod: %s\ndata: %s\nresp: %s" % (
      url, method, data, resp))
    if resp is not None:
      err_str = resp.text

    return None, err_str

class SecretRotatorUtil(object):
  XI_MGMT_CONFIG = "/appliance/logical/xi_mgmt/config"
  SECRET_STORE_AUDIENCE_URL = "https://secretstore.us-east-1.xi.nutanix.com/"
  SECRET_STORE_URL = "secretstore.dmz.xi.nutanix.com"
  ZK_SESSION_RETRY_COUNT = 2
  RETRY_WAIT_TIME = 10

  def __init__(self):
    self.__set_tenant_uuid()
    self.__set_cluster_uuid()
    self.__set_secret_store_context()

  """
  Method to fetch secrets from secret store
  """
  def get_secret_store_secret(self, secret_path):
    iam_token = self.__get_iam_token()

    secret_store = SecretStoreHelper(self.SECRET_STORE_URL, iam_token, port=443, verify_session=False)

    err = "Unknown error in getting secret from secret store"
    for _ in retry_with_delay(1000, max_retries=5):
      try:
        result, err = secret_store.get(secret_path)
        if not result:
          return None, err
        return result, None
      except SecretStoreHelperException as ex:
        err = "Failed to get secret. Error: {0}".format(str(ex))
        log.debug(err)
        log.info("Retrying...")
    return None, err

  """
  Method to fetch secret store key pair. Returns decoded keys.
  """
  def get_secret_store_key_pair(self):
    # private key
    log.info("Fetching secret store private key at {0}".format(self._secret_store_private_key_path))

    private_key_data, error = self.get_secret_store_secret(self._secret_store_private_key_path)
    if error == 404:
      raise SecretNotFoundException("Private key not found in secret store")
    if not private_key_data:
      log.error("Error in fetching private key. Exiting ..")
      sys.exit(1)

    secret_private_key = \
      private_key_data[SecretStoreHelper.VAULT_SECRET_DATA_KEY]
    secret_private_key_encoding = \
      private_key_data[SecretStoreHelper.VAULT_SECRET_ENCODING_KEY]
    if secret_private_key_encoding == "base64":
      secret_private_key = base64.b64decode(secret_private_key)

    # public key
    log.info("Fetching secret store public key at {0}".format(self._secret_store_public_key_path))

    public_key_data, error = self.get_secret_store_secret(self._secret_store_public_key_path)
    if error == 404:
      raise SecretNotFoundException("Public key not found in secret store")
    if not public_key_data:
      log.error("Error in getting public key. Exiting ..")
      sys.exit(1)

    secret_public_key = \
      public_key_data[SecretStoreHelper.VAULT_SECRET_DATA_KEY]
    secret_public_key_encoding = \
      public_key_data[SecretStoreHelper.VAULT_SECRET_ENCODING_KEY]
    if secret_public_key_encoding == "base64":
      secret_public_key = base64.b64decode(secret_public_key)

    log.info("Got the secret store key pair successfully")

    return secret_private_key, secret_public_key

  """
  Method determines if all nodes have the same secret as secret store.
  Returns a list of nodes that have incorrect secret
  """
  def nodes_with_incorrect_secrets(self):
    # Get secret store secrets
    try:
      secret_store_private_key, secret_store_public_key = self.get_secret_store_key_pair()
    except SecretNotFoundException as e:
      log.error(str(e))
      # Secret store has no secret, exit with error
      sys.exit(1)
  
    set_of_nodes_with_incorrect_secrets = set()
    svmips = genesis_utils.get_svm_ips()

    local_secrets = self.__get_local_secrets()

    private_keys = run_cmd_on_all_cvms(svmips, "cat {}".format(local_secrets['private_key_path']))
    public_keys = run_cmd_on_all_cvms(svmips, "cat {}".format(local_secrets['public_key_path']))

    for ip, (_, local_private_key, _) in private_keys.items():
      if local_private_key != secret_store_private_key:
        set_of_nodes_with_incorrect_secrets.add(ip)

    for ip, (_, local_public_key, _) in public_keys.items():
      if local_public_key != secret_store_public_key:
        set_of_nodes_with_incorrect_secrets.add(ip)
      
    return set_of_nodes_with_incorrect_secrets

  """
  Method to fetch secrets from secret store and rotate them locally
  """
  def rotate_secrets(self, force_rotate=False, svmips=None):
    if force_rotate:
      log.info("force_rotate is set.")
      try:
        secret_private_key, secret_public_key = self.get_secret_store_key_pair()
      except SecretNotFoundException as e:
        log.error(str(e))
        # Secret store has no secret, exit with error
        sys.exit(1)

      # Get local secrets
      local_secrets = self.__get_local_secrets()
      private_key_filename = os.path.basename(local_secrets['private_key_path'])
      public_key_filename = os.path.basename(local_secrets['public_key_path'])
      private_key_dir = os.path.dirname(local_secrets['private_key_path'])
      public_key_dir = os.path.dirname(local_secrets['public_key_path'])
      temp_private_key_location = os.path.join(os.environ.get("deployment_working_dir","/home/nutanix/tmp"), private_key_filename)
      temp_public_key_location = os.path.join(os.environ.get("deployment_working_dir","/home/nutanix/tmp"),public_key_filename)

      log.info("Writing private key to temp file at {0}".format(temp_private_key_location))
      with open(temp_private_key_location,
                'w') as private_k_file:
        private_k_file.write(secret_private_key)

      log.info("Writing public key to temp file at {0}".format(temp_public_key_location))
      with open(temp_public_key_location,
                'w') as public_key_file:
        public_key_file.write(secret_public_key)

      # Get all cvm ips that need secrets to be rotated
      if not svmips:
        svmips = genesis_utils.get_svm_ips()

      log.info("rotating secrets for {0}".format(svmips))

      # Get current node under deployment
      current_node = os.environ.get("CANAVERAL_TARGET_HOST")

      # Copy the key pair to every node in the cluster
      # {0} current_node
      # {1} temp_private_key_path
      # {2} svmp_ip
      # {3} target_dir/
      # cmd : scp <remote>:foo target_dir/
      log.info("Copying new private key to all nodes")
      cmd_to_run = "scp -v {0}:{1} {2}/ 2>&1".format(current_node,
                      temp_private_key_location,
                      private_key_dir)
      log.info("private key scp command is {0}".format(cmd_to_run))
      log.info("Copying new private key to all nodes")
      scp_run_result = run_cmd_on_all_cvms(svmips, cmd_to_run)
      if scp_run_result:
        log.info("Successfully copied private key to all nodes")
      else:
        log.error("Failed to copy private key to all nodes.")
        sys.exit(1)

      log.info("Copying new public key to all nodes")
      cmd_to_run = "scp -v {0}:{1} {2}/ 2>&1 ".format(current_node,
                      temp_public_key_location,
                      public_key_dir)
      log.info("public key scp command is {0}".format(cmd_to_run))
      log.info("Copying new public key to all nodes")
      scp_run_result = run_cmd_on_all_cvms(svmips, cmd_to_run)
      if scp_run_result:
        log.info("Successfully copied public key to all nodes")
      else:
        log.error("Failed to copy public key to all nodes.")
        sys.exit(1)

      # Remote the temporary copy of keys
      log.info("removing temp copy of private and public keys")
      os.remove(temp_private_key_location)
      os.remove(temp_public_key_location)

      # Restart aplos and cfs services
      cluster_health_svc = "ClusterHealthService"
      aplos_svc = "APLOSService"
      aplos_engine_svc = "APLOSEngineService"
      cerebro_svc = "CerebroService"
      atlas_svc = "AtlasService"
      magneto_svc = "MagnetoService"
      cluster_type = "pc" if genesis_utils.is_pc_vm() else "pe"

      if cluster_type == "pc":
        restart_services = [aplos_svc, aplos_engine_svc, cluster_health_svc,
                            atlas_svc, magneto_svc]
      else:
        restart_services = [aplos_svc, aplos_engine_svc, cluster_health_svc,
                            cerebro_svc]

      if not service_lifecycle(False, services_list=restart_services):
        log.error("Failed to stop services. Exiting..")
        sys.exit(1)
      if not service_lifecycle(True, services_list=restart_services):
        log.error("Failed to start services. Exiting..")
        sys.exit(1)

      log.info("Successfully rotated local secret keys")
    else:
      log.info("force_rotate is not set. Finding nodes that need secret rotation.")
      list_of_nodes_with_incorrect_secrets = self.nodes_with_incorrect_secrets()
      log.info("Nodes that need secret rotation: {0}".format(list_of_nodes_with_incorrect_secrets))
      if list_of_nodes_with_incorrect_secrets:
        self.rotate_secrets(force_rotate=True, svmips=list_of_nodes_with_incorrect_secrets)
      else:
        log.info("Local keys are same as secret store keys for all nodes. Exiting..")
        sys.exit(0)

  """
  Method to save local secrets to secret store
  """
  def save_secrets(self, force_save=False):
    def _update_secret(secret_path, secret, secret_type, encoding=None):
      iam_token = self.__get_iam_token()
      secret_store = SecretStoreHelper(self.SECRET_STORE_URL,
                                          iam_token, port=443,
                                          verify_session=False)
      err = "Unknown error in updating secret to secret store"
      for _ in retry_with_delay(1000, max_retries=5):
        try:
          result, err = secret_store.update(secret_path, secret, secret_type,
                                            encoding, "internet,corpnet,dc")
          if not result:
            return None, err
          return secret_path, None
        except SecretStoreHelperException as ex:
          err = "Failed to save secret. Error: {0}".format(str(ex))
          log.debug(err)
          log.info("Retrying...")
      return None, err

    local_secrets = self.__get_local_secrets()

    local_private_key = open(local_secrets['private_key_path'], 'r').read()
    local_public_key = open(local_secrets['public_key_path'], 'r').read()

    if force_save:
      log.info("force_save is set. Executing force_save..")

      # Base64 Encode local secrets
      log.info("encoding local secrets to base64")
      local_private_key_encoded = base64.b64encode(local_private_key)
      local_public_key_encoded = base64.b64encode(local_public_key)

      service_uuid = local_secrets['service_account_uuid']

      log.info("Updating secret store private key at {0}".format(
        self._secret_store_private_key_path
      ))

      path, error = _update_secret(self._secret_store_private_key_path,
                                   local_private_key_encoded, "ssl+private", encoding="base64")
      if not path:
        log.error("Error in updating private key. Exiting ..")
        sys.exit(1)

      log.info("Updating secret store public key at {0}".format(
        self._secret_store_public_key_path
      ))

      path, error = _update_secret(self._secret_store_public_key_path,
                                   local_public_key_encoded, "ssl+public", encoding="base64")
      if not path:
        log.error("Error in updating public key. Exiting ..")
        sys.exit(1)

      log.info("Updating secret store uuid at {0}".format(
        self._secret_store_uuid_path
      ))

      path, error = _update_secret(self._secret_store_uuid_path,
                                   service_uuid, "string")
      if not path:
        log.error("Error in updating service account uuid. Exiting ..")
        sys.exit(1)

      log.info("Successfully pushed secrets to secret store. Exiting..")
      sys.exit(0)

    # Get secret store secrets
    try:
      secret_store_private_key, secret_store_public_key = self.get_secret_store_key_pair()
    except SecretNotFoundException as e:
      log.error(str(e))
      # Secret store has no secret, force save the local secret in this case
      self.save_secrets(force_save=True)

    # If local keys are same as the ones in secret store, do nothing and pass; else fail
    if local_private_key == secret_store_private_key and local_public_key == \
      secret_store_public_key:
      log.info("Local keys are same as secret store keys. No operation performed. Exiting..")
      sys.exit(0)
    else:
      log.error("Mismatch between local and secret store keys. Exiting..")
      sys.exit(1)

  """
  Method to get iam token
  """
  def __get_iam_token(self):
    token_client = TokenClient(self.__get_iam_config())
    token, err = token_client.request_token(self.SECRET_STORE_AUDIENCE_URL,
                                            domain="xi-prod")
    if not token:
      raise Exception(
        "Could not get the token from iam with the audience %s due to %s" % (
          self.SECRET_STORE_AUDIENCE_URL, err))
    log.info("Got the IAM token successfully")
    return token

  """
  Method to get iam config from local environ variables
  """
  def __get_iam_config(self):
    uuid_path = os.environ['SERVICE_IAM_UUID_FILE']

    iam_config = {
      "service_account_uuid": open(uuid_path, 'r').read(),
      "iam_host": "iam.dmz.xi.nutanix.com",
      "port": 443,
      "public_key_path": os.environ['SERVICE_IAM_PUBLIC_KEY_FILE'],
      "private_key_path": os.environ['SERVICE_IAM_PRIVATE_KEY_FILE']
    }

    return iam_config

  """
  Method to get locally saved secrets
  """
  def __get_local_secrets(self):
    zk_path = "/appliance/logical/iam/mgmt_plane/service_account_config"
    zk_session = self._get_zk_session()
    if not zk_session:
      log.error("Error in getting zk session for getting local secrets. Exiting ..")
      sys.exit(1)

    zk_node = zookeeper_interface.ZkNode(zk_session, zk_path)
    serialized_info = zk_node.get()
    iam_config_proto = IamServiceAccountConfig()
    iam_config_proto.ParseFromString(serialized_info)
    log.info("Got the local secrets successfully.")
    return pb2json(iam_config_proto)

  """
  Method to set secret store paths for the secrets
  """
  def __set_secret_store_context(self):
    cluster_type = "pc" if genesis_utils.is_pc_vm() else "pe"
    if os.environ.get('pipeline') is not None:
      domain = "xi-{0}".format(os.environ['pipeline'])
    else:
      log.error("pipeline variable not set. Exiting ...")
      sys.exit(1)

    path_prefix = "/{0}/services/xi-tenants/{1}/{2}/{3}/iam/service_account/" \
                  "".format(domain, self._tenant_uuid, cluster_type,
                            self._cluster_uuid)
    self._secret_store_private_key_path = "{0}private_key".format(path_prefix)
    self._secret_store_public_key_path = "{0}public_key".format(path_prefix)
    self._secret_store_uuid_path = "{0}uuid".format(path_prefix)

  def _get_zk_session(self):
    for count in range(1, self.ZK_SESSION_RETRY_COUNT+1):
      try:
        zk_session = NutanixInterfaces().zk_session
        return zk_session
      except NutanixInterfacesError:
        log.error("Error in getting zk session in attempt {0}".format(str(count)))
        if count < self.ZK_SESSION_RETRY_COUNT:
          log.info("Retrying again to get zk session after retry wait time ...")
          time.sleep(self.RETRY_WAIT_TIME)
        else:
          log.error("All attempts failed in getting zk session.")

    return None

  """
  Method to retrieve and set tenant uuid
  """
  def __set_tenant_uuid(self):
    zk_session = self._get_zk_session()
    if not zk_session:
      log.error("Error in getting zk session for getting tenant uuid. Exiting ..")
      sys.exit(1)

    zk_node = zookeeper_interface.ZkNode(zk_session, self.XI_MGMT_CONFIG)
    xi_config = XiPcConfig()
    xi_config.ParseFromString(zk_node.get())
    self._tenant_uuid = xi_config.tenant_uuid

  """
  Method to retrieve and set cluster uuid
  """
  def __set_cluster_uuid(self):
    cluster_command = "source /etc/profile;zeus_config_printer | grep " \
                      "'cluster_uuid' | awk {'print $2'} | sed 's/\"//g' | uniq"
    self._cluster_uuid = os.popen(cluster_command).read().strip()
    log.info("Successfully got the cluster uuid {0}".format(self._cluster_uuid))

class CanaveralClient():
  CANAVERAL_CONFIG = {
    "url": "https://canaveral-engine-api.canaveral-corp.us-west-2.aws",
    "log_url": "https://canaveral-artifacts.corp.nutanix.com",
    "token_url": "https://canaveral.nutanix.com.internal",
    "domain": "xi-prod"
  }

  def __init__(self, context_id=None):
    self._context_id = context_id if context_id else str(uuid.uuid4())
    self._canaveral_token = self.__get_iam_token()

  def __get_iam_token(self):
    """
    Method to get iam token
    """
    token_client = TokenClient(self.__get_iam_config())
    token, err = token_client.request_token(self.CANAVERAL_CONFIG["token_url"],
      domain=self.CANAVERAL_CONFIG["domain"])

    if not token:
      raise Exception(
        "Could not get the token from iam with the audience %s due to %s" % (
          self.CANAVERAL_CONFIG["token_url"], err))
    print("Got the IAM token successfully")
    return token

  def __get_iam_config(self):
    """
    Method to get iam config from local environ variables
    """
    uuid_path = os.environ['SERVICE_IAM_UUID_FILE']

    iam_config = {
      "service_account_uuid": open(uuid_path, 'r').read(),
      "iam_host": "iam.dmz.xi.nutanix.com",
      "port": 443,
      "public_key_path": os.environ['SERVICE_IAM_PUBLIC_KEY_FILE'],
      "private_key_path": os.environ['SERVICE_IAM_PRIVATE_KEY_FILE']
    }

    return iam_config

  def get_canaveral_status_url_suffix(self, org_name, service_name, context_id):
    """
    Prepares the Canaveral deployment context URL for GET
    Args:
      org_name: (str) Organization e.g. xi-dc-infra
      service_name: (str) Service e.g. dcim-dcm
      context_id: (str) Context UUID
    Returns:
      (str): Contexts URL
    """
    suffix = ("services/%s/%s/deployments/contexts/"
              "%s" % (org_name, service_name, context_id))
    return suffix

  def get_canaveral_deployment_status(self, org_name, service_name, context_id,
                                      num_retries=10, wait=30):
    """
    Fetches the Canaveral Deployment status
    Args:
      org_name: (str) Organization
      service_name: (str) Service Name
      context_id: (str) Context UUID
      num_retries: (int) Retry count
      wait: (int) Timeout to the call
    Returns:
      (dict) : Deployment JSON
    """
    suffix = self.get_canaveral_status_url_suffix(org_name, service_name,
      context_id)
    endpoint = self.CANAVERAL_CONFIG["url"]
    url = self.__get_connection_url(endpoint, suffix)
    headers = self.__get_engine_headers()
    log.info("Canaveral URL: %s" % url)
    try:
      response = requests.request("GET", url, headers=headers, verify=False)
      if response is None or response.status_code >=400:
        log.error("Request for url %s filed with response %s" % (url, response))
        return {}
      log.info("Successfully retrieved deployment status")
      return response.json()

    except Exception:
      log.info("Exception in fetching deployment status: %s"
                % traceback.format_exc())
    return {}

  def __get_engine_headers(self):
    """
    Returns the header for canaveral deployment request.
    """
    return {
      "Authorization": "Bearer {0}".format(self._canaveral_token)
    }

  def __get_connection_url(self, endpoint, suffix):
    return '{0}/{1}'.format(endpoint, suffix)

  def wait_for_canaveral_deployment(self, org_name, service_name, context_id,
                                    num_retries=60, wait=60,
                                    pending_timeout=300):
    """
    Blocking wrapper that waits for deployment to finish
    """
    poll_count = 1
    status = None
    dep_data = {}
    while num_retries:
      try:
        dep_data = self.get_canaveral_deployment_status(org_name, service_name,
                                                   context_id)
        status = dep_data["status"]["status"]
      except KeyError as err:
        log.error(err.message)
        time.sleep(wait)
        num_retries -= 1
        poll_count += 1
        continue

      if status == "completed::succeeded":
        log.info("Canaveral Deployment succeeded. Log: %s"
                 % self.get_canaveral_log_url(dep_data.get('deployment_id')))
        return True
      elif status in ("error", "completed::failed"):
        log.info("Canaveral Deployment failed. Log: %s"
                 % self.get_canaveral_log_url(dep_data.get('deployment_id')))
        return False
      else:
        log.info("%s retry count: %s | Current status: %s"
                 % (context_id, poll_count, status))
        time.sleep(wait)
        num_retries -= 1
        poll_count += 1

      if poll_count * wait >= pending_timeout and status == "pending":
        log.error("%s deployment is in %s state for long, Failing deployment"
                  % (service_name, status))
        return False

    log.error("Timeout waiting for %s deployment; Last status: %s"
            % (service_name, status))
    return False

  def get_canaveral_deployment_url_suffix(self, org, service):
    """
    Attaches the org and service name to the deployment URL suffix
    Args:
      org: (str) Organization
      service: (str) Service
    Returns:
      (str) URL
    """
    suffix = "services/{0}/{1}/deployments".format(
      org, service)
    return suffix
  def deploy_canaveral_package(self, org_name, service_name, template_data,
                               package_name, context_id, pipeline, wait=120):
    # Check if context id was used before.
    log.info("Deploying %s/%s -> %s using canaveral with context_id %s" %
             (org_name, service_name, package_name, context_id))

    try:
      # TO check for deployment start with a short retry count.
      try:
        status = self.get_canaveral_deployment_status(org_name, service_name,
          context_id, wait=30,num_retries=3)["status"]["status"]

        if status in ("error", "completed::failed"):
          # Deployment already failed for this context id.
          return False
        return True

      except KeyError as err:
        log.info("Ignoring error: %s" % err.message)

    except Exception:
      log.info("Exception in fetching deployment status: %s"
                  % traceback.format_exc())
      # Continue with deployment in this case, the deployment was never started
      # in this case
      return {}

    suffix = self.get_canaveral_deployment_url_suffix(org_name, service_name)
    body = {
      'pipeline': pipeline,
      'package_id': package_name,
      'context_id': context_id,
      'data': template_data
    }
    endpoint = self.CANAVERAL_CONFIG["url"]
    headers = self.__get_engine_headers()
    url = self.__get_connection_url(endpoint, suffix)
    log.info("Canaveral URL: %s" % url)
    try:
      response = requests.request("PUT", url, headers=headers, json=body,
                                 verify=False)
      if response is None or response.status_code !=200:
        log.error("Request for url %s filed with response %s" % (url, response))
        return {}
      log.info("Successfully started package deployment.")
      return response.json()

    except Exception:
      log.error("Exception in package deployment deployment. Status: %s"
                % traceback.format_exc())
    return {}

  def get_canaveral_log_url(self, deployment_id):
    """
    Prepares the Canaveral log link for a particular deployment
    Args:
      deployment_id: (str) canaveral deployment id
    Returns:
      (str) : Log URL to artifacts
    """
    suffix = "/artifacts/canaveral-private/{deployment_id}/".format(
      deployment_id=deployment_id)
    return self.CANAVERAL_CONFIG["log_url"] + suffix
