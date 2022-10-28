# -*- coding: utf-8 -*-
from setuptools import setup
from setuptools.command.install import install

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        # PUT YOUR POST-INSTALL SCRIPT HERE or CALL A FUNCTION
        import pyraphtory_jvm
        pyraphtory_jvm.jre.check_dl_java_ivy()

setup_kwargs = {
    'cmdclass': {
                 'install': PostInstallCommand,
             },
}

setup(**setup_kwargs)
