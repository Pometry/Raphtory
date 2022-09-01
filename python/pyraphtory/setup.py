# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
    ['pyraphtory',
     'pyraphtory.algorithms',
     'pyraphtory.scala',
     'pyraphtory.scala.implicits']

package_data = \
    {'': ['*']}

install_requires = \
    ['cloudpickle>=0.4', 'pandas>=1.4.3', 'pemja>=0.2.5', 'py4j>=0.10,<0.11']

setup_kwargs = {
    'name': 'pyraphtory',
    'version': '0.0.1',
    'description': 'Raphtory - Temporal Graph Analytics Platform. This is the Python version of the library.',
    'long_description': '# Getting started with `pyraphtory`\n\nPyraphtory is the new version of the Raphtory python API. It is in experimental stage, so everything might change.\n\n## Running the PyRaphtory sample locally\n\nAt the core of this iteration is the `com.raphtory.python.PyRaphtory` class that is able to start a Raphtory instance\nwith python support or connect to an existing Raphtory Cluster (more on that later)\n\n## Install\n\nPlease install via conda. Please see the [Raphtory](https://github.com/raphtory/raphtory) Github for more information.\n\n\n### Run the PyRaphtory class\n\n1. start pulsar and proxy `make local-pulsar`\n2. (in a different shell) run pyraphtory\n\n```bash\ncurl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv\n# using make\nmake INPUT=/tmp/lotr.csv PYFILE=python/pyraphtory/sample.py BUILDER=LotrGraphBuilder pyraphtory-local\n# or directly in bash\njava -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory --file=$(INPUT) --py=$(PYFILE) --builder=$(BUILDER) --mode=$(MODE)\n```\n\n## Connect to a running cluster (advanced)\n\n1. Follow the `Prelude` and `Setup a python virtual environment with dependencies` from the `Running the PyRaphtory sample locally` guide.\n2. Open the Makefile and inspect the `run-local-cluster` step, ensure the data is available for the spout and the python file is available for the builder\n3. Open the `bin/docker/raphtory/docker-compose.yml` file and edit `RAPHTORY_SPOUT_FILE`, `PYRAPHTORY_GB_FILE`, `PYRAPHTORY_GB_CLASS` environment variables\n4. run\n\n```bash\njava -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory \\\n  --py python/pyraphtory/sample.py \\\n  --connect="raphtory.pulsar.admin.address=http://localhost:8080,raphtory.pulsar.broker.address=pulsar://127.0.0.1:6650,raphtory.zookeeper.address=127.0.0.1:2181"\n```\n\n## Links\n\nPyPi https://pypi.org/project/pyraphtory/\n\nGithub https://github.com/Raphtory/Raphtory/\n\nWebsite https://raphtory.github.io/\n\nSlack https://raphtory.slack.com\n\nDocumentation https://raphtory.readthedocs.io/\n\nBug reports/Feature request https://github.com/raphtory/raphtory/issues\n',
    'author': 'Pometry',
    'author_email': 'admin@pometry.com',
    'maintainer': 'Pometry',
    'maintainer_email': 'admin@pometry.com',
    'url': 'https://raphtory.com/',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.10,<3.11',
}


setup(**setup_kwargs)
