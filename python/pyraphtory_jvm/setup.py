from setuptools import setup, find_packages
from setuptools.command.install import install
from distutils.command.build import build
import os

class BuildCommand(build):
    def run(self):
        print("Build_wheel")
        import pyraphtory_jvm
        pyraphtory_jvm.jre.check_dl_java_ivy(download_dir=os.getcwd()+'/pyraphtory_jvm/data/')
        build.run(self)

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        # PUT YOUR POST-INSTALL SCRIPT HERE or CALL A FUNCTION
        # import pyraphtory_jvm
        # pyraphtory_jvm.jre.check_dl_java_ivy()

packages = find_packages()

package_data = \
    {'': ['*']}

install_requires = \
    ['requests']

setup_kwargs = {
    'name': 'pyraphtory_jvm',
    'version': '0.2.0a5',
    'description': 'Bootstrap JRE and IVY installer for Pyraphtory',
    'url': 'https://github.com/raphtory/raphtory',
    'author': 'Haaroon Yousaf',
    'author_email': 'admin@pometry.com',
    'maintainer': 'Pometry',
    'maintainer_email': 'admin@pometry.com',
    'license': 'Apache 2.0',
    'packages': packages,
    'install_requires': install_requires,
    'include_package_data' : True,
    'package_data': package_data,
    'cmdclass': {
        'build': BuildCommand,
        # 'install': PostInstallCommand,
    },
    'python_requires': '>=3.9.13,<3.11',
    'classifiers': [
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
    ]
}

setup(**setup_kwargs)
