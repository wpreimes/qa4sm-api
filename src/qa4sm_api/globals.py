from pathlib import Path
import os
import  warnings
import configparser


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
    Read credentials from a .qa4smapirc file.

    Parameters
    ----------
    path : str or Path, optional (default: Path.home())
        Directory that contains the dotrc file.
    name : str, optional (default: ".qa4smapirc")
        Name of the dotrc file to load.

    Returns
    -------
    config : dict
        Credentials keyed by hostname, e.g.:
        {
            "qa4sm.eu":      {"token": "..."},
            "test.qa4sm.eu": {"token": "..."},
        }
        Sections named 'default' or 'qa4sm' both map to 'qa4sm.eu'.
        All other section names (e.g. 'test', 'test2') map to '<name>.qa4sm.eu'.
    """
    path_dotrc = Path(path) / name

    if not path_dotrc.exists():
        raise FileNotFoundError(
            f'{name} file not found at {path}. '
            f'Please check https://qa4sm.eu/ui/public-api'
        )

    PRODUCTION_SECTIONS = {"default", "qa4sm", "qa4sm.eu"}

    def section_to_host(section: str) -> str:
        return "qa4sm.eu" if section.lower() in PRODUCTION_SECTIONS \
            else section

    parser = configparser.ConfigParser()
    parser.read(path_dotrc)

    config = {}

    # Handle DEFAULT first — named sections will override it if they resolve
    # to the same host (e.g. [qa4sm] overrides [DEFAULT] for 'qa4sm.eu')
    if parser.defaults():
        config[section_to_host("default")] = dict(parser.defaults())

    for section in parser.sections():
        config[section_to_host(section)] = dict(parser[section])

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
if ("QA4SM_INSTANCE" in os.environ) and ("QA4SM_TOKEN" in os.environ):
    QA4SM_ACCESS = {os.environ["QA4SM_INSTANCE"]: os.environ["QA4SM_TOKEN"]}
else:
    try:
        QA4SM_ACCESS = load_dotrc(path, name)
    except FileNotFoundError:
        QA4SM_ACCESS = None

TOKEN_WARNING = (
    f"Could not derive QA4SM token from {name} file in {path}. "
    f"Continue without token (limited API access)."
)

TOKEN_ERROR = ConnectionError(
    "No valid token provided. Please check the docs. Provide a valid token, "
    "set the QA4SM_TOKEN environment variable, or create a .qa4smapirc file "
    "with your token."
)