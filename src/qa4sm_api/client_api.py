import json
import warnings
import requests
import time
import pandas as pd
from typing import Union
import os
import zipfile
from pathlib import Path
from datetime import datetime
from tornado.httpclient import HTTPError
from qa4sm_api.globals import (
    QA4SM_ACCESS,
    ValidationRunNotFoundError,
    TOKEN_WARNING, TOKEN_ERROR
)


class Response:
    def __init__(self, response, serialize=True):
        self.response = response
        if serialize:
            self.response = self.response.json()

    @property
    def data(self) -> list[dict]:
        if self.response is None:
            raise ValueError("No response data is available")
        elif isinstance(self.response, dict):
            response = [self.response]
        else:
            response = self.response

        return response

    @property
    def pandas(self) -> Union[pd.DataFrame, pd.Series]:
        """ Get all request return data as a DataFrame """
        df = pd.DataFrame.from_dict(self.data, orient='columns') \
                           .set_index('id') \
                           .sort_index()
        if len(df.index) == 1:
            return df.iloc[0, :]  # dtype: pd.Series
        else:
            return df  # dtype: pd.DataFrame


class Session:
    """
    Wrapper to send API request to QA4SM after authentication.
    """
    def __init__(self, instance="qa4sm.eu", token="file", protocol="https"):
        """
        Session to send requests to QA4SM via API.

        Parameters
        ----------
        instance: str, optional (default: 'qa4sm.eu')
            Base URL to send API requests to
        token: str, optional (default: None)
            To authenticate with QA4SM API pass your token.
            While some request work without authentication, all
            user-specific request need you to log in first.
            - "auto" (default) will try "file" first and continue with "none" otherwise.
            - "file" uses the token from the ~/.qa4smapirc file (error if not found).
            - "none" to force not setting a token even if .qa4smapirc is found.
            - "<token>" pass a valid token to use it directly (without checking .qa4smapirc)
        protocol: Literal['http', 'https'], optional (default: 'https')
            Developer setting. Use https for the public instances (test, prod)
            or http for local instances.
            e.g., 127.0.0.1:8000
        """
        self.headers = {
            "Content-Type": "application/json"
        }
        self.instance = instance
        self.base_url = f"{protocol}://{self.instance}/"
        self.api_url = self.base_url + "api/"

        self.client = requests.Session()
        self.response = None

        if token == "auto":
            if QA4SM_ACCESS is None:
                token = 'none'
                warnings.warn(TOKEN_WARNING)
        elif token == "file":
            if QA4SM_ACCESS is None:
                raise TOKEN_ERROR
            else:
                token = QA4SM_ACCESS[self.instance]['token']
        elif token.lower() == 'none':
            token = None
        else:  # token direct
            token = token

        self.user = None
        if token is not None:
            _ = self.login_with_token(token)
        else:
            warnings.warn("No token was passed, limited API access. "
                          "Only public API calls will work.")

    def url(self, *args) -> str:
        # Join URL parts
        args = [str(a) for a in args]
        url = '/'.join(args).replace('//', '/')
        if url.endswith('/'):
            url = url[:-1]

        return self.api_url + url

    def login_with_token(self, token, quiet=False) -> int:
        """
        status_code
        200: OK, login successful
        """
        self.headers["Authorization"] = f"Token {token}"
        re = self.get(self.url("auth/login"), headers=self.headers)
        username = re.pandas['username']
        if not quiet:
            print(f"Hi, {username}! You're successfully logged "
                  f"in at {self.api_url}!")
        self.user = username
        return 200

    def login_with_credentials(self, username=None, password=None,
                               quiet=False):
        """
        Authenticate with username and password to receive a token for
        subsequent requests.

        Parameters
        ----------
        username: str
            Username for the chosen QA4SM instance.
        password: str
            Password for the chosen QA4SM instance.
        quiet: bool, optional (default: False)
            Suppress welcome message.
        """
        data = {'username': username, 'password': password}
        response = self.post(self.url("auth/login"), data=data)
        token = response.pandas['auth_token']
        self.login_with_token(token, quiet=quiet)


    def _send_request(self,
                      url,
                      data=None,
                      max_retries: int = 5,
                      wait_time_s: float = 0.1,
                      serialize=True,
                      **kwargs) -> Response:
        """
        Send request. Usually happens already during initialisation.
        Except when the request is delayed. Will try again when a request
        fails temporarily.
        """
        for attempt in range(max_retries):
            try:
                if data is None:
                    response = requests.get(url, timeout=10, **kwargs)
                else:
                    if self.headers is None:
                        raise ValueError("No headers found to post request.")
                    response = requests.post(url, headers=self.headers,
                                             json=data, timeout=10, **kwargs)
                response.raise_for_status()
                response = Response(response, serialize=serialize)
                self.response = response
                return response

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:  # Last attempt
                    raise e

                time.sleep(wait_time_s)

    def post(self, url, data, *args, **kwargs) -> Response:
        return self._send_request(url, data, *args, **kwargs)

    def get(self, url, *args, **kwargs) -> Response:
        return self._send_request(url, *args, **kwargs)


class ValidationConfiguration:
    def __init__(self, config_data: dict):
        self.data = config_data[0] if len(config_data) == 1 else config_data

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        if key not in self.data.keys():
            raise KeyError(f"{key} is not in the configuration.")
        self.data[key] = value

    def __eq__(self, other):
        return self.data == other.data

    def dump(self, path):
        """
        Dump the configuration to a new json file.

        Parameters
        ----------
        path: str or Path
            Path to json file to create
        """
        with open(Path(path), 'w') as jfile:
            json.dump(self.data, jfile, indent=2)

    @classmethod
    def from_remote(cls, run_id, **connection_kwargs):
        """
        Load a configuration from an existing remote run. Configs are
        public, so it's not necessary to authenticate.

        Parameters
        ----------
        run_id: str
            UID of the validation run at the chosen instance
            (see connection_kwargs).
        instance: str, optional (default: "qa4ms.eu")
            see qa4sm_api.client_api.Connection
        token: str, optional
            see qa4sm_api.client_api.Connection
        protocol: str, optional
            see qa4sm_api.client_api.Connection
        """
        connection = Connection(**connection_kwargs)
        config = connection.download_configuration(run_id=run_id)
        return cls(config_data=config.data)


    @classmethod
    def from_file(cls, path):
        """
        Load a configuration from a json file.

        Parameters
        ----------
        path: str or Path
            Path to the json file to load
        """
        with open(Path(path), 'r', encoding='utf-8') as file:
            data = json.load(file)

        return cls(data)


class Connection:
    """
    Communication with QA4SM.
    """
    def __init__(self, instance: str="qa4sm.eu", token="file",
                 protocol="https"):
        """
        Parameters
        ----------
        instance: str, optional (default: "qa4ms.eu")
            service URL or IP:PORT, e.g
            - qa4sm.eu [productive]
            - test.qa4sm.eu [test]
            - test2.qa4sm.eu [test2]
            - 0.0.0.0:8000 [develop]
        token: str, optional
            Authentication user token (required to POST)
            - "file" will search for a token in a .qa4smapirc file
            - "none" will force not using a token (only public commands)
            - "<token>" will use the passed token directly
        protocol: str, optional
            Developer setting. Use https for the public instances (test, prod)
            or http for local instances.
            e.g., 127.0.0.1:8000
        """
        self.session = Session(instance, token, protocol)

    def url(self, *args, **kwargs) -> str:
        return self.session.url(*args, **kwargs)

    def login(self, username, password):
        self.session.login_with_credentials(username, password)

    def user(self) -> pd.Series:
        re = self.session.get(self.url("auth/login"),
                              headers=self.session.headers)
        return re.pandas

    def dataset_id(self, short_name: str) -> int:
        datasets = self.datasets()
        idx = datasets.index[datasets['short_name'] == short_name]
        if len(idx) == 0:
            raise ValueError(f"The dataset {short_name} was not found. "
                             f"Please pass a valid name or ID. "
                             f"`Check Connection.datasets()`")
        elif len(idx) > 1:
            raise ValueError(f"Multiple datasets {short_name} found. "
                             f"Please pass a unique name or ID.")
        else:
            idx = idx[0]

        return int(idx)

    def datasets(self) -> pd.DataFrame:
        r = self.session.get(self.url("dataset"))
        df = r.pandas
        return df

    def versions(self, ds) -> pd.DataFrame:
        """
        Get the version information for a dataset.

        Parameters
        ----------
        ds: int or str
            The dataset index or short name to get versions for

        Returns
        -------
        df: pd.DataFrame
            Version information for the chosen dataset
        """
        ds_id = self.dataset_id(ds) if isinstance(ds, str) else ds

        datasets = self.datasets()
        version_ids = datasets.loc[ds_id, 'versions']

        dfs = []
        for vid in version_ids:
            r = self.session.get(self.url("dataset-version", vid))
            row = r.pandas.T.to_frame().T
            dfs.append(row)

        return pd.concat(dfs, axis=0).sort_index()

    def dataset_info(self, ds_id) -> pd.Series:
        ds = self.datasets().loc[ds_id]
        ds['id'] = ds_id
        return ds

    def version_info(self, vers_id) -> pd.Series:
        re = self.session.get(self.url("dataset-version", vers_id))
        ds = re.pandas
        ds['id'] = vers_id
        return ds

    def variable_info(self, var_id) -> pd.Series:
        re = self.session.get(self.url("dataset-variable", var_id))
        ds = re.pandas
        ds['id'] = var_id
        return ds

    def filter_info(self, filter_id):
        re = self.session.get(self.url("data-filter"))
        ds = re.pandas
        ds = ds.loc[filter_id, :]
        ds['id'] = filter_id
        return ds

    def get_period(self, vers_id: int) -> (str, str):
        """
        Get start and end date of selected dataset directly from the service
        """
        ds = self.version_info(vers_id)
        return ds["time_range_start"], ds["time_range_end"]

    def check_errors(self, validation_id):
        """Check if the passed validation run has ended with an error"""
        # TODO: Should be implemented in API
        raise NotImplementedError()

    def _remote_val_status(self, validation_id):
        url = self.url(f"validation-runs-status/{validation_id}")
        response = self.session.get(url, headers=self.session.headers).data[0]
        return response

    def _remote_timing(self, validation_id):
        url = self.url(f"validation-runs-timing/{validation_id}")
        response = self.session.get(url, headers=self.session.headers).data[0]
        return response

    def validation_exists(self, validation_id: str) -> bool:
        """
        Check if a validation run exists online (running or finished, not
        deleted).
        """
        try:
            _ = self._remote_val_status(validation_id)
        except HTTPError:
            return False
        return True

    def validation_time(self, validation_id: str) -> \
            (Union[datetime, None], Union[datetime, None]):
        """
        Get start and end time when a validation run was processing.
        This works for finished OR running validations.

        Returns
        -------
        start_time: datetime or None
            None means that the validation was not started
        """
        if not self.validation_exists(validation_id):
            raise ValidationRunNotFoundError(validation_id)
        else:
            response = self._remote_timing(validation_id)
            start_time = pd.to_datetime(response["start_time"]).to_pydatetime()
            end_time = response["end_time"]
            if end_time is not None:
                end_time = pd.to_datetime(end_time).to_pydatetime()

            return start_time, end_time

    def validation_duration(self, validation_id: str) -> (int, str):
        """
        Get the duration of a validation run in seconds and formatted string.
        This works for finished OR running validations
        """
        if not self.validation_exists(validation_id):
            raise ValidationRunNotFoundError(validation_id)
        else:
            url = self.url(f"validation-runs-timing/{validation_id}")
            response = self.session.get(url,
                                        headers=self.session.headers).data[0]

            duration_seconds = response["duration_seconds"]
            duration_format = response["duration_format"]

            return duration_seconds, duration_format

    def validation_status(self, validation_id):
        """
        Check if the passed validation run is still running, completed or
        is not found.

        Parameters
        ----------
        validation_id: str
            Hash of the remote validation run to check.

        Returns
        -------
        status: str
           Status of the validation run. Can be one of:
            - 'NOT FOUND': The validation id was not found
            - 'SCHEDULED': The validation is queued
            - 'RUNNING': The validation is still running
            - 'DONE': The validation is completed
            - 'CANCELLED': The validation was cancelled
            - 'ERROR': The validation failed with an error
        progress: int
            Progress of the validation run in percent (0-100).
        """
        exists = self.validation_exists(validation_id)

        if not exists:
            return "NOT FOUND", 0
        else:
            response = self._remote_val_status(validation_id)
            status = response['status']
            progress = response['progress']
            return status, progress

    def run_validation(self, config):
        """
        Trigger validation run based on the passed config.

        Parameters
        ----------
        config: ValidationConfiguration
            Validation configuration to send to the service

        Returns
        -------
        response: pd.Series
            Response from validation run
        """
        re = self.session.post(self.url("start-validation"), data=config.data)

        return re.pandas

    def download_configuration(self, run_id, out_dir=None):
        """
        Download validation configuration used for a specific run.):

        Parameters
        ----------
        run_id: str
            UID of remote run to download configuration for
        out_dir: str, optional
            To save the config as a .json file, pass the storage path

        Returns
        -------
        config: ValidationConfiguration
            Downloaded Configuration object
        """
        url = self.url(f"validation-configuration/{run_id}")
        response = self.session.get(url)
        config = ValidationConfiguration(response.data)
        if out_dir is not None:
            config.dump(os.path.join(out_dir, f"{run_id}.json"))
        return config

    def download_results(self, run_id, out_dir, force_download=False):
        """
        Download all results for a run

        Parameters
        ----------
        run_id: str
            UID of remote run to download results for
        out_dir: str or Path
            Where the results are stored, will be created if it doesn't exist
            yet.
        force_download: bool, optional
            Always download, replace any existing local files.
            If False, only downloads results that don't exist locally.
        """
        out_dir = Path(out_dir)

        params = {
            "validationId": run_id,
            "fileType": "graphics",
        }

        os.makedirs(out_dir, exist_ok=True)

        graphx_dir = os.path.join(out_dir, "qa4sm_graphics")
        if force_download or (not os.path.exists(graphx_dir)):
            re = self.session.get(self.url("download-result"), serialize=False,
                                  params=params, stream=True)
            file_out = os.path.join(out_dir, "graphics.zip")
            with open(file_out, "wb") as file:
                for chunk in re.response.iter_content(chunk_size=8192):
                    file.write(chunk)

            with zipfile.ZipFile(file_out, 'r') as zip_ref:
                zip_ref.extractall(graphx_dir)
            # Remove .zip after extraction
            os.remove(file_out)

        params["fileType"] = "netCDF"
        file_out = os.path.join(out_dir, f"{run_id}.nc")
        if force_download or (not os.path.exists(file_out)):
            re = self.session.get(self.url("download-result"), serialize=False,
                                  params=params, stream=True)
            with open(file_out, "wb") as file:
                for chunk in re.response.iter_content(chunk_size=8192):
                    file.write(chunk)

        _ = params.pop("fileType")
        file_out = os.path.join(out_dir, f"summary_stats.csv")
        if force_download or (not os.path.exists(file_out)):
            re = self.session.get(self.url("download-statistics-csv"), serialize=False,
                                  params=params, stream=True)
            with open(file_out, "wb") as file:
                for chunk in re.response.iter_content(chunk_size=8192):
                    file.write(chunk)

    def run_config_validation(self, config_path, override=None):
        """
        Trigger validation run based on the passed config.

        Parameters
        ----------
        config_path: str
            Path to the config json to post.
        override: dict, optional (default: None)
            keys and values to override settings in the configuration.

        Returns
        -------
        response: dict
            Response from validation run (or config if dry_run is True)
        """
        config = ValidationConfiguration.from_file(config_path)

        if override is not None:
            for k, v in override.items():
                if k not in config.data:
                    raise KeyError(f"{k} does not exist in config.")
                else:
                    config.data[k] = v

        response = self.run_validation(config)

        return response


if __name__ == '__main__':

    QA4SM_IP_OR_URL = "test.qa4sm.eu"   # "127.0.0.1:8000"  #
    QA4SM_API_TOKEN = "2b37740a1f6733c9cfc2e1e105abe974ff8c4204"
    username = "preimesberger"
    password = "i7j.r308"
    # conn.login(username, password)
    qa4sm = Connection(QA4SM_IP_OR_URL, token='file')
    qa4sm = Connection(QA4SM_IP_OR_URL, QA4SM_API_TOKEN, protocol='https')
    qa4sm.session
    qa4sm.download_configuration("332f3873-a5bc-4df2-b8b2-0e025096f83e",
                                 "/tmp")

    status, p = qa4sm.validation_status("6ec9ced3-3938-4f32-aec7-992bb1dba478")
    start, end = qa4sm.validation_time("6ec9ced3-3938-4f32-aec7-992bb1dba478")

    qa4sm.user()
    qa4sm.filter_info(1)
    qa4sm.version_info(1)
    qa4sm.versions(1)
    qa4sm.versions("C3S_combined")
    qa4sm.get_period(1)

    qa4sm.run_config_validation(
        "./configs/smos_l2_v700/01-SmosL2-vs-C3sComb-abs.json",
                  override={"name_tag": "testdtest"}
    )
    qa4sm.download_results("6ec9ced3-3938-4f32-aec7-992bb1dba478",
                          "/tmp/test")

