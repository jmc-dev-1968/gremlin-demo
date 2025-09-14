
import boto3
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
import ssl
import config
import time
import datetime

"""

these utilities facilitate development on the Neptune cluster from a local machine (Windows in my case)
the architecture of Neptune requires your client (e.g. Sagemaker/Jupyter Notebook, EC2) reside within the VPC
of Neptune. a (laborious) workaround is to use a bastion host (EC2) to create an SSH tunnel locally (I'm using Putty for this),
so gremlin queries (via websocket on wss://localhost:8182/gremlin) and neptune loading (via http over https://localhost:8182')
can be performed from your local machine. in addition, some AWS  security features need set up as well (IAM User, IAM Role etc.)

TODO : if in PROD or DEV (while using EC2 within VPC) create branching logic for much simplfied http/ws calls below (sigv4 signing/ssl 
are not needed). key this off of config.py var USE_SSH_TUNNEL = False 

"""

class NeptuneWSConnection:

    def __init__(self):

        self._connection = None
        self._setup_connection()

    def _setup_connection(self):

        # aws creds
        session = boto3.Session(
            aws_access_key_id = config.AWS_ACCESS_KEY,
            aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY,
            region_name = config.AWS_REGION
        )

        credentials = session.get_credentials()
        host = "{}:{}".format(config.NEPTUNE_URL, config.NEPTUNE_PORT)
        request = AWSRequest(
            method = 'GET',
            url = 'wss://localhost:8182/gremlin',
            # override host header (neptune validates signature against host header, not actual connection endpoint)
            headers = {'Host': host}
        )
        # add sigv4 header info to request
        SigV4Auth(credentials, 'neptune-db', config.AWS_REGION).add_auth(request)

        # ssl context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # create traversal source connection
        self._connection = DriverRemoteConnection(
            'wss://localhost:8182/gremlin',
            'g',
            headers=dict(request.headers),
            ssl_context=ssl_context
        )

    def get_traversal(self):
        return traversal().with_remote(self._connection)

    def close(self):
        if self._connection:
            self._connection.close()


class NeptuneHTTPConnection:

    def __init__(self):

        self._session = None
        self._headers = {}
        self._setup_session()
        self._refresh_headers()

    def _setup_session(self):

        self._session = boto3.Session(
            aws_access_key_id = config.AWS_ACCESS_KEY,
            aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY,
            region_name = config.AWS_REGION
        )

    def _refresh_headers(self):

        credentials = self._session.get_credentials()
        host = "{}:{}".format(config.NEPTUNE_URL, config.NEPTUNE_PORT)
        request = AWSRequest(
            method = 'POST',
            url = 'https://localhost:8182/loader',
            # override host header (neptune validates signature against host header, not actual connection endpoint)
            headers = {'Host': host}
        )
        # add sigv4 header info to request
        SigV4Auth(credentials, 'neptune-db', config.AWS_REGION).add_auth(request)
        self._headers = dict(request.headers)


    def _add_custom_headers_event(self, request, **kwargs):

        self._refresh_headers()
        for key, value in self._headers.items():
            request.headers[key] = value


    def load_csv(self, prefix=""):

        # TODO if ssh tunnel not used, use the full name of the neptune server
        #endpoint_url = "https://{}:{}".format(config.NEPTUNE_URL, config.NEPTUNE_PORT)
        endpoint_url = 'https://localhost:8182'

        client_config = Config(
            read_timeout = 600,  # in secs
            connect_timeout = 60
        )

        neptune_client = self._session.client(
            'neptunedata',
            endpoint_url = endpoint_url,
            verify = False, # disable ssl
            config = client_config
        )
        neptune_client.meta.events.register('before-sign.neptunedata.*', self._add_custom_headers_event)

        if prefix == "":
            source = "s3://{}/{}".format(config.S3_BUCKET, config.S3_PREFIX)
        else:
            source = "s3://{}/{}/{}".format(config.S3_BUCKET, config.S3_PREFIX, prefix)

        start_time = time.perf_counter()
        try:

            response = neptune_client.start_loader_job(
                source = source,
                format = 'csv',
                s3BucketRegion = config.AWS_REGION,
                iamRoleArn = config.IAM_ROLE_ARN,
            )

            #print("response success")

        except Exception as e:

            print("reponse failed: {}".format(e))
            exit()

        load_id = response['payload']['loadId']
        print("load job started (ID: {})".format(load_id))

        # TODO : use proper asycnch processing using asyncio, threading etc.
        total_seconds = 0
        seconds_increment = 5
        abort_seconds = 300
        while True:

            time.sleep(seconds_increment)
            total_seconds += seconds_increment
            # check status
            status_response = neptune_client.get_loader_job_status(loadId = load_id)
            status = status_response['payload']['overallStatus']['status']
            print("load status: {}".format(status))
            if status == "LOAD_COMPLETED":
                break
            elif status != "LOAD_COMPLETED" and total_seconds >= abort_seconds:
                print("load failed to finish, aborting ".format(load_id))
                break
            else:
                continue

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        elapsed_datetime = datetime.datetime(1900, 1, 1) + datetime.timedelta(seconds=elapsed_time)
        formatted_time = elapsed_datetime.strftime('%H:%M:%S')
        print("load time: {}".format(formatted_time))

