from pyraphtory import __version__

def test_version():
    assert __version__ == '0.2.0.alpha'

if __name__ == '__main__':
    test_version()