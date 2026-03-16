from pathlib import Path
import os
import  warnings


class ValidationRunNotFoundError(ValueError):

    def __init__(self, id):
        self.message = (f"Validation run {id} not found on current "
                        f"QA4SM instance.")
        super().__init__(self.message)


class ValidationRunError(Exception):

    def __init__(self, message="Validation run failed"):
        self.message = message
        super().__init__(self.message)


def load_dotrc(path=Path.home(), name='.qa4smapirc') -> dict:
    """
    Read FTP login credentials from .smosrc file.

    Parameters
    ----------
    path: str, optional (default: Path.home())
        Directory that contains the dotrc file.
    name: str, optional (default: ".qa4smapirc")
        Name of the dotrc file to load.

    Returns
    -------
    config: dict
        Elements from the dotrc file
    """
    path_dotrc = Path(path) / name

    if not os.path.exists(path_dotrc):
        raise FileNotFoundError(f'{name} file not found at {path}. '
                                f'Please check https://qa4sm.eu/ui/public-api')
    config = {}
    with open(path_dotrc) as f:
        for line in f.readlines():
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            # Parse key: value or key=value pairs
            if ':' in line:
                key, value = line.split(':', 1)
                config[key.strip()] = value.strip()
            elif '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()

    return config

#######################################

# QA4SM_DOTC contains the path to the API dotrc file
if "QA4SM_DOTRC" in os.environ:
    QA4SM_DOTRC = Path(os.environ["QA4SM_DOTRC"])
    path = QA4SM_DOTRC.parent
    name = QA4SM_DOTRC.name
else:
    path, name = Path.home(), ".qa4smapirc"
    QA4SM_DOTRC = Path(path) / name

# QA4SM_TOKEN contains the token loaded from the dotrc file or None

if "QA4SM_TOKEN" in os.environ:
    QA4SM_TOKEN = Path(os.environ["QA4SM_TOKEN"])
else:
    try:
        config = load_dotrc(path, name)
        QA4SM_TOKEN = config["token"]
    except FileNotFoundError:
        QA4SM_TOKEN = None

TOKEN_WARNING = (
    f"Could not derive QA4SM token from {name} file in {path}. "
    f"Continue without token (limited API access)."
)

TOKEN_ERROR = ConnectionError(
    "No valid token provided. Please check the docs. Provide a valid token, "
    "set the QA4SM_TOKEN environment variable, or create a .qa4smapirc file "
    "with your token."
)