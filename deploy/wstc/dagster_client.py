# TEMP(wstc-minimal): the WSTC team's client (from Kai), vendored unchanged so the notebook runs anywhere.
"""Use the WSTC Dagster from a Jupyter notebook (or any Python): upload code, run it, get results.

Only needs `requests`. Talks to Dagster's GraphQL API at https://wstc-dagster.nrp-nautilus.io with
the dashboard's basic-auth password. No Kubernetes access is needed.

    from dagster_client import Dagster
    d = Dagster(password="...")             # or set DAGSTER_PASSWORD
    d.upload("my_pipeline.py")              # copy a file into Dagster's shared code folder + reload
    d.jobs()                                # ['catalog_summary_job', 'my_job', ...]
    run_id = d.launch("my_job")             # or: run_id, status, results = d.run("my_job")
    d.wait(run_id)                          # 'SUCCESS' / 'FAILURE' / 'CANCELED'
    print(d.url_for(run_id))                # open the run in the browser
    d.results(run_id)                       # {asset: {label: value}} recorded by the steps
    d.logs(run_id)                          # {step: printed output}
    d.errors(run_id)                        # why steps failed
    d.load_errors()                         # files that could not be loaded, with the reason
    d.remove("my_pipeline.py")              # take the file out again
"""
import json
import os
import time

import requests

URL = "https://wstc-dagster.nrp-nautilus.io"
LOCATION = "wstc-code"
REPOSITORY = "__repository__"
SYSTEM_JOBS = ("wstc_upload_code", "wstc_remove_code")


class Dagster:
    def __init__(self, password=None, user="admin", url=URL):
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (user, password or os.environ["DAGSTER_PASSWORD"])

    def query(self, query, **variables):
        r = self.session.post(f"{self.url}/graphql", json={"query": query, "variables": variables},
                              timeout=60)
        r.raise_for_status()
        body = r.json()
        if body.get("errors"):
            raise RuntimeError(body["errors"])
        return body["data"]

    def reload(self):
        """Re-read the shared code folder. Returns the location's status (LOADED or an error)."""
        data = self.query("""mutation($name: String!) { reloadRepositoryLocation(repositoryLocationName: $name) {
            __typename ... on WorkspaceLocationEntry { locationOrLoadError { __typename
              ... on PythonError { message } } } ... on PythonError { message } } }""", name=LOCATION)
        result = data["reloadRepositoryLocation"]
        error = result.get("message") or (result.get("locationOrLoadError") or {}).get("message")
        if error:
            raise RuntimeError(error)
        return "LOADED"

    def jobs(self):
        data = self.query("""query($loc: String!, $repo: String!) {
            repositoryOrError(repositorySelector: {repositoryLocationName: $loc, repositoryName: $repo}) {
              ... on Repository { jobs { name } } ... on PythonError { message } } }""",
                          loc=LOCATION, repo=REPOSITORY)
        repo = data["repositoryOrError"]
        if "message" in repo:
            raise RuntimeError(repo["message"])
        return [j["name"] for j in repo["jobs"]
                if not j["name"].startswith("__") and j["name"] not in SYSTEM_JOBS]

    def upload(self, path, filename=None):
        """Save a local .py file into Dagster's shared code folder (replacing a file with the same
        name) and reload. Returns the job list. Raises with the reason if the file is rejected."""
        with open(path) as f:
            code = f.read()
        filename = filename or os.path.basename(path)
        self._system_run("wstc_upload_code", "wstc_write_code", {"filename": filename, "code": code})
        self.reload()
        broken = self.load_errors().get(os.path.splitext(filename)[0])
        if broken:
            raise RuntimeError(f"{filename} was saved but could not be loaded:\n{broken}")
        return self.jobs()

    def remove(self, filename):
        """Delete one of your files from the shared code folder and reload. Returns the job list."""
        self._system_run("wstc_remove_code", "wstc_delete_code", {"filename": os.path.basename(filename)})
        self.reload()
        return self.jobs()

    def _system_run(self, job, op, config):
        run_id = self.launch(job, run_config={"ops": {op: {"config": config}}})
        if self.wait(run_id, poll=3, timeout=600) != "SUCCESS":
            raise RuntimeError("; ".join(self.errors(run_id)) or f"{job} failed: {self.url_for(run_id)}")

    def load_errors(self):
        """{file name without .py: error} for files in the shared folder that could not be loaded."""
        data = self.query("""query($loc: String!, $repo: String!) {
            repositoryOrError(repositorySelector: {repositoryLocationName: $loc, repositoryName: $repo}) {
              ... on Repository { assetNodes { assetKey { path } description groupName } } } }""",
                          loc=LOCATION, repo=REPOSITORY)
        out = {}
        for node in data["repositoryOrError"].get("assetNodes", []):
            name = node["assetKey"]["path"][-1]
            if node["groupName"] == "load_errors" and name.startswith("load_error_"):
                out[name[len("load_error_"):]] = node["description"]
        return out

    def launch(self, job, run_config=None, tags=None):
        """Start a run of `job`; returns its run id. run_config is the same dict as in the Launchpad."""
        data = self.query("""mutation($params: ExecutionParams!) { launchRun(executionParams: $params) {
            __typename ... on LaunchRunSuccess { run { runId } }
            ... on PythonError { message } ... on RunConfigValidationInvalid { errors { message } }
            ... on InvalidSubsetError { message } ... on PipelineNotFoundError { message } } }""",
                          params={"selector": {"repositoryLocationName": LOCATION,
                                               "repositoryName": REPOSITORY, "jobName": job},
                                  "runConfigData": run_config or {},
                                  "executionMetadata": {"tags": [{"key": k, "value": str(v)}
                                                                 for k, v in (tags or {}).items()]}})
        result = data["launchRun"]
        if result["__typename"] != "LaunchRunSuccess":
            raise RuntimeError(result)
        return result["run"]["runId"]

    def status(self, run_id):
        data = self.query("""query($id: ID!) { runOrError(runId: $id) {
            ... on Run { status } ... on RunNotFoundError { message } } }""", id=run_id)
        return data["runOrError"].get("status") or data["runOrError"]["message"]

    def wait(self, run_id, poll=10, timeout=3600):
        """Block until the run finishes; returns its final status."""
        end = time.time() + timeout
        while time.time() < end:
            status = self.status(run_id)
            if status in ("SUCCESS", "FAILURE", "CANCELED"):
                return status
            time.sleep(poll)
        raise TimeoutError(f"run {run_id} still {status} after {timeout} s")

    def results(self, run_id):
        """Metadata each asset recorded in this run (context.add_output_metadata), as
        {asset: {label: value}}. The returned Python objects themselves stay on Dagster's shared
        volume (`path`); for real data, have the step write files to S3 and read them from there."""
        data = self.query("""query($id: ID!) { runOrError(runId: $id) { ... on Run {
            assetMaterializations { assetKey { path } metadataEntries { label
              ... on JsonMetadataEntry { jsonString } ... on IntMetadataEntry { intValue }
              ... on FloatMetadataEntry { floatValue } ... on TextMetadataEntry { text }
              ... on PathMetadataEntry { path } ... on UrlMetadataEntry { url }
              ... on MarkdownMetadataEntry { mdStr } ... on BoolMetadataEntry { boolValue } } } } } }""",
                          id=run_id)
        out = {}
        for m in data["runOrError"].get("assetMaterializations", []):
            entries = {}
            for e in m["metadataEntries"]:
                value = next((v for k, v in e.items() if k != "label"), None)
                entries[e["label"]] = json.loads(value) if "jsonString" in e else value
            out["/".join(m["assetKey"]["path"])] = entries
        return out

    def run(self, job, run_config=None, tags=None, timeout=3600):
        """Launch, wait and collect: returns (run_id, status, results)."""
        run_id = self.launch(job, run_config=run_config, tags=tags)
        status = self.wait(run_id, timeout=timeout)
        return run_id, status, self.results(run_id)

    def _events(self, run_id):
        data = self.query("""query($id: ID!) { runOrError(runId: $id) { ... on Run { eventConnection { events {
            __typename ... on MessageEvent { message stepKey }
            ... on ExecutionStepFailureEvent { error { message causes { message } } }
            ... on RunFailureEvent { pipelineFailureError: error { message } }
            ... on LogsCapturedEvent { fileKey stepKeys } } } } } }""", id=run_id)
        return data["runOrError"]["eventConnection"]["events"]

    def errors(self, run_id):
        """Error messages of the failed steps (and of the run), most useful first."""
        out = []
        for e in self._events(run_id):
            if e["__typename"] == "ExecutionStepFailureEvent":
                err = e.get("error") or {}
                causes = [c["message"].strip() for c in err.get("causes") or []]
                out.append(f"{e['stepKey']}: " + (causes[-1] if causes else (err.get("message") or e["message"]).strip()))
            elif e["__typename"] == "RunFailureEvent" and not out:
                out.append(e["message"])
        return out

    def logs(self, run_id):
        """{step: what it printed (stdout, then stderr incl. context.log lines)}."""
        out = {}
        for e in self._events(run_id):
            if e["__typename"] != "LogsCapturedEvent":
                continue
            data = self.query("""query($key: [String!]!) { capturedLogs(logKey: $key) { stdout stderr } }""",
                              key=[run_id, "compute_logs", e["fileKey"]])
            logs = data["capturedLogs"]
            for step in e["stepKeys"] or ["run"]:
                out[step] = (logs.get("stdout") or "") + (logs.get("stderr") or "")
        return out

    def url_for(self, run_id):
        return f"{self.url}/runs/{run_id}"
