from setuptools import setup, find_packages
from distutils.command.build import build
import os

class BuildCommand(build):
    def run(self):
        print("Build_wheel")
        from pyraphtory_jvm import jre
        jre.check_dl_java_ivy(download_dir=os.getcwd()+'/pyraphtory_jvm/data/')
        build.run(self)


packages = find_packages()

package_data = \
    {'': ['*']}

requires = \
    ['requests']

setup_kwargs = {
    'name': 'pyraphtory_jvm',
    'version': '0.1.4',
    'description': 'Bootstrap JRE and IVY installer for Pyraphtory',
    'url': 'https://github.com/raphtory/raphtory',
    'author': 'Haaroon Yousaf',
    'author_email': 'admin@pometry.com',
    'maintainer': 'Pometry',
    'maintainer_email': 'admin@pometry.com',
    'license': 'Apache 2.0',
    'packages': packages,
    'setup_requires': requires,
    'install_requires': requires,
    'include_package_data' : True,
    'package_data': package_data,
    'cmdclass': {
        'build': BuildCommand,
    },
    'python_requires': '>=3.9.13,<3.11',
    'classifiers': [
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
    ],
    'test_suite': "tests.test_pyraphtory_jvm"
}

setup(**setup_kwargs)
