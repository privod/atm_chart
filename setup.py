from setuptools import setup, find_packages
from os.path import join, dirname

import project

setup(
    name='atm_chart',
    version=project.__version__,
    packages=find_packages(),
    long_description=open(join(dirname(__file__), 'README.rst')).read(),

    install_requires=[
        'bokeh',
        'flask',
        'pandas',
    ],
    # entry_points={
        # 'console_scripts': [
            # 'actual_ncr = project.main:start',
        # ]
    # },
    # include_package_data=True,
    # test_suite='tests',
)