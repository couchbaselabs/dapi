import sys
import argparse
import json
import logging
import constants

log = logging.getLogger()

class RestfulDAPI:
    def __init__(self, dapi_endpoint, access_token, access_secret, test, scope, collection):
        self.dapi_endpoint = dapi_endpoint
        self.payload = {"username": access_token, "password": access_secret}
        self.check_dapi_health()
        if test == "health":
            self.check_dapi_health()
        if test == "read_collection_docs":
            self.read_collection_docs(scope, collection)


    def print_and_exit(self, msg=None, err=None):
        if err:
            log.error(err)
            sys.exit(1)
        else:
            print(msg)
            sys.exit(0)

    def check_dapi_health(self):
        url = self.dapi_endpoint + "/health"
        status = self.execute_curl_command(self.payload, url)
        if not status or status.strip() != '{"health":"OK"}':
            self.print_and_exit(err="DAPI Health Check Failed! \n{}".format(status))
        else:
            self.print_and_exit(msg="DAPI Health Check Passed \n{}".format(status))

    # curl -X GET -H 'Content-Type:application/json' -u 'yHggV1871PDb5Vw3lJF603xQb1oQUTPK:eaTqYzg7n9yEXELSwbQiPNA4fGGzahDiP2GtLPsKNdIMmD8eSmWs5vySlyohUCYk' 'https://riteshElixir-o48ukb.data.dev.nonprod-project-avengers.com
    # /v1/scopes/_default/collections/_default/docs?pretty=true&logs=true'
    def create_doc(self, doc_id, doc_content={}, scope="_default", collection="_default"):
        payload = doc_content
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
              + collection + "/docs/" + doc_id
        self.execute_curl_command(payload=payload, url=url, post=True)

    def read_doc(self, doc_id, scope, collection, extra_params):
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
              + collection + "/docs/" + doc_id + extra_params
        self.execute_curl_command(payload=self.payload, url=url)

    def read_collection_docs(self, scope, collection, extra_params):
        url = self.dapi_endpoint + "/scopes/" + scope + "/collections/" \
              + collection + "/docs" + extra_params
        status = self.execute_curl_command(payload=self.payload, url=url)
        print(status)
        if not status:
            self.print_and_exit(err="Read Collection Docs Failed! \n{}".format(status))
        else:
            self.print_and_exit(msg="Read Collection Docs Passed \n{}".format(status))

    def update_doc(self):
        pass

    def delete_doc(self):
        pass

    def get_collection_docs(self):
        pass

    def create_primary_index(self):
        pass

    def execute_query(self):
        pass

    def execute_curl_command(self, payload, url, post=False, timeout_sec=30):
        import subprocess
        import shlex
        from threading import Timer
        if post:
            cmd = "curl -X POST -H Content-Type:application/json -d'" + json.dumps(payload) + "' " + url
        else:
            # GET
            cmd = "curl -X -H Content-Type:application/json " + url

        cmd_args = shlex.split(cmd)
        process = subprocess.Popen(cmd_args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        timer = Timer(timeout_sec, process.kill)
        try:
            timer.start()
            stdout, stderr = process.communicate()
        finally:
            timer.cancel()
        if process.returncode != 0:
            log.error("Error executing curl command : \"{0}\"\n{1}".format(cmd, stderr.decode('utf-8')))
            return None
        return stdout.decode('utf-8')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dapi_endpoint',
                        help="Data API endpoint for DB", type=str, required=True)
    parser.add_argument('--access_token', help='DB access token', type=str, required=True)
    parser.add_argument('--access_secret', help='DB access secret', type=str, required=True)
    parser.add_argument('--test', choices=constants.TESTS, default="health")
    parser.add_argument('--scope', default="_default", type=str)
    parser.add_argument('--collection', default="_default", type=str)
    parser.add_argument('--extra_params', default="?pretty=true&logs=true", type=str)

    args = parser.parse_args()
    RestfulDAPI(args.dapi_endpoint, args.access_token, args.access_secret, args.test,
                args.scope, args.collection)
