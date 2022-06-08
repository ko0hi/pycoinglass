# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['pycoinglass']

package_data = \
{'': ['*']}

install_requires = \
['pandas>=1.4.2,<2.0.0', 'requests>=2.27.1,<3.0.0']

setup_kwargs = {
    'name': 'pycoinglass',
    'version': '0.1.0',
    'description': '',
    'long_description': None,
    'author': 'ko0hi',
    'author_email': 'ko0hi.4731@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.9.0,<4.0.0',
}


setup(**setup_kwargs)
