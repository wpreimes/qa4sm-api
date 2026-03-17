import os
from tempfile import TemporaryDirectory
from qa4sm_api.globals import load_dotrc

content = "[DEFAULT]\ntoken: asdf123\n\n[test.qa4sm.eu]\ntoken: hjkl456\nsomethingelse: 123"

def test_load_credentials_file():
    with TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, '.qa4smapirc')
        with open(path, 'w') as f:
            f.write(content.strip())
        dotrc = load_dotrc(tmpdir, '.qa4smapirc')

    assert 'DEFAULT' not in dotrc.keys()
    assert len(dotrc.keys()) == 2
    assert dotrc['test.qa4sm.eu']['token'] == 'hjkl456'
    assert dotrc['test.qa4sm.eu']['somethingelse'] == '123'
    assert dotrc['qa4sm.eu']['token'] == 'asdf123'
