import glob
import warnings
import yaml
import pandas as pd
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import shutil
import os
from pathlib import Path
import xarray as xr
import re

from qa4sm_api.client_api import Connection, ValidationConfiguration
from qa4sm_api.extent import GeographicExtent


class ValidationRun:
    def __init__(
            self,
            config: ValidationConfiguration,
            root_local: Union[str, Path],
            connection: Connection,
            remote_id=None,
    ):
        """
        Parameters
        ----------
        config: ValidationConfiguration
            Configuration for validation run to trigger (settings)
        root_local: Union[str, Path]
            Local root folder, a subfolder for the validation run is created
        connection: Connection
            Connection to the QA4SM instance to run the validation on
        remote_id: str, optional
            Remote ID if the run already exists online
        """
        self.config = config
        self.root_local = Path(root_local)
        self.connection = connection
        self.remote_id = remote_id
        self.name = self.update_name(self.config['name_tag'])

    def __eq__(self, other) -> bool:
        return self.remote_id == other.remote_id

    @property
    def extent(self) -> (float, float, float, float):
        # y_min, x_min, y_max, x_max
        d = self.config.data
        extent = GeographicExtent.from_corners(d['min_lat'], d['min_lon'],
                                               d['max_lat'], d['max_lon'])
        return extent

    @property
    def url(self):
        """Get the API URL of the validation run."""
        if self.remote_id is None:
            return None
        else:
            return self.connection.url(f"validation-configuration/{self.remote_id}")

    def get_results_url(self):
        """Get the UI URL of the validation run."""
        if self.remote_id is None:
            return None
        else:
            url = self.connection.url(f"validation-result/{self.remote_id}")
            return url.replace('api', 'ui')

    def get_reference(self, reftype='spatial'):#
        """
        Get reference dataset for this run.

        Parameters
        ----------
        reftype: Literal['spatial', 'temporal', 'scaling']
            What scaling reference to get

        Returns
        -------
        dataset: str
            Dataset name
        version: str
            Version name
        variable: str
            Variable name
        """
        for conf in self.config.data["dataset_configs"]:
            if conf[f'is_{reftype}_reference']:
                dataset = int(conf['dataset_id'])
                version = int(conf['version_id'])
                variable = int(conf['variable_id'])
                dataset = self.connection.dataset_info(dataset)['pretty_name']
                version = self.connection.version_info(version)['pretty_name']
                variable = self.connection.variable_info(variable)['pretty_name']
                return str(dataset), str(version), str(variable)

        return None, None, None

    @classmethod
    def from_remote(cls, local_root: Union[str, Path], connection: Connection,
                    remote_id: str):
        """
        Set up ValidationRun based on a remote validation run with a local
        folder for synchronization.

        Parameters
        ----------
        local_root: str
            Local folder where the run data is stored
        connection: Connection
            Service connection for your user
        remote_id: str
            Name of the remote run (UID).

        Returns
        -------
        run : ValidationRun
        """
        local_root = Path(local_root)
        url = connection.url(f"validation-configuration/{remote_id}")
        response = connection.session.get(url)
        config = ValidationConfiguration(response.data[0])
        cls._init_origin = 'remote'

        return cls(config, local_root, connection, remote_id)

    @classmethod
    def from_local(cls, local_dir: Union[str, Path],
                   connection: Connection):
        """
        Set up ValidationRun based on a previously synchronized, now local, run.

        Parameters
        ----------
        local_dir: Union[str, Path]
            Local path where the configs are stored
        connection: Connection
            Connection to QA4SM instance where the run was started.

        Returns
        -------
        run : ValidationRun
        """
        local_dir = Path(local_dir)
        conf_file = glob.glob(str(local_dir / "config.json"))
        assert len(conf_file) == 1, \
            f"Found multiple config files in {local_dir}"
        conf_file = conf_file[0]

        config = ValidationConfiguration.from_file(conf_file)

        results_files = glob.glob(str(local_dir / "*.nc"))
        response_file = glob.glob(str(local_dir / "response-*.csv"))

        if len(results_files) > 0:
            assert len(results_files) == 1, \
                f"Found multiple results netcdf files in {local_dir}"
            remote_id = os.path.basename(results_files[0]).split('.nc')[0]
        elif len(response_file) > 0:
            assert len(response_file) == 1, \
                f"Found multiple response csv files in {local_dir}"
            response_file = response_file[0]
            response = pd.read_csv(response_file, index_col=0).squeeze()
            remote_id = response['pk']
        else:
            raise ValueError("Could not detect results run ID from local "
                             "contents. Check if a results or response file"
                             "containing the UUID are available.")
        cls._init_origin = 'local'

        return cls(config, root_local=local_dir,
                   connection=connection, remote_id=remote_id)

    def load_results(self) -> xr.Dataset:
        """
        Load downloaded results as xarray.
        """
        ds = xr.open_dataset(self.root_local / f'{self.remote_id}.nc')
        return ds

    def update_remote_id(self, pk):
        if self.response is not None:
            self.remote_id = pk
            return self.remote_id

    def update_name(self, new_name: str):
        self.config['name_tag'] = new_name
        self.name = self.config['name_tag']
        return self.name

    def setup_workdir(self, clear=False):
        if self.root_local.exists() and clear:
            shutil.rmtree(self.root_local)
        os.makedirs(self.root_local, exist_ok=True)

    def override_params(self, **kwargs):
        """
        Override certain parameters in the validation config file. Such as
        name_tag and start/end date etc.

        Parameters
        ----------
        kwargs:
            Keys and new values. Keys must already exist in the config. You
            cannot add anything new, only change existing fields!
        """
        for k, v in kwargs.items():
            self.config[k] = v

    def verify_period(self):
        """
        Checks if the chosen validation period is within the range available
        for all datasets on the service.

        Returns
        -------
        status: bool
            True if all datasets are available, False otherwise
        """
        period_start = pd.to_datetime(self.config['interval_from'])
        period_end = pd.to_datetime(self.config['interval_to'])

        for ds_config in self.config['dataset_configs']:
            avail_start, avail_end = self.connection.get_period(
                ds_config['version_id'])
            avail_start = pd.to_datetime(avail_start)
            avail_end = pd.to_datetime(avail_end)

            if (period_start < avail_start) or (period_end > avail_end):
                return False

        return True

    def start(self):
        """
        Start the current Validation Run on the chosen instance. Creates
        a local folder and dumps the config and the response from the server
        there.

        Returns
        -------
        response: dict
            Response from validation run
        """
        self.setup_workdir(clear=True)
        self.response = self.connection.run_validation(self.config)
        run_pk = self.response['pk']
        self.config.dump(self.root_local / f'config.json')
        self.response.to_csv(self.root_local / f'response-{run_pk}.csv')
        self.update_remote_id(self.response['pk'])
        return self.response

    def timing(self) -> dict:
        """
        Get timing information for the remote validation run

        Returns
        -------
        time: dict
            Time information as a dict
        """
        status, progress = self.status()

        time = {'start': None, 'end': None, 'duration': None}

        if status == 'NOT FOUND':
            pass
        else:
            start_time, end_time = (
                self.connection.validation_time(self.remote_id))
            _, duration = self.connection.validation_duration(self.remote_id)
            time['start'] = start_time
            time['end'] = end_time
            time['duration'] = duration

        return time

    def status(self) -> Tuple[str, int]:
        """
        Check the status of the remote run.

        Returns
        -------
        status[str], progress[int]
            see :func:`Connection.validation_status`
        """
        return self.connection.validation_status(self.remote_id)

    def download_data(self, force_download=False):
        """
        Download the run's results, i.e., netcdf file, plots.

        Parameters
        ----------
        force_download: bool, optional
            Always download, replace any existing local files.
            If False, only downloads results that don't exist locally.
        """
        os.makedirs(self.root_local, exist_ok=True)
        self.config.dump(self.root_local / f'config.json')
        self.connection.download_results(self.remote_id, self.root_local,
                                         force_download=force_download)

    def plot_extent(self):
        """
        Create a map plot of the area covered by the validation run.
        """
        os.makedirs(self.root_local, exist_ok=True)
        path = self.root_local
        fig = self.extent.plot_map()
        fig.savefig(path / "extent.png", bbox_inches='tight')


if __name__ == '__main__':
    QA4SM_IP_OR_URL = "test.qa4sm.eu"
    QA4SM_API_TOKEN = "2b37740a1f6733c9cfc2e1e105abe974ff8c4204"
    qa4sm = Connection(QA4SM_IP_OR_URL, QA4SM_API_TOKEN)
    name1 = "01-SmosL2-vs-C3sComb-abs"
    id1 = "6eb61199-59b8-4ecc-8e3c-7b1139df4a05"
    run = ValidationRun.from_remote('/tmp', qa4sm, id1)

