import os

import setuptools

requirements = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'requirements.txt')
with open(requirements) as f:
    install_requires = f.read().splitlines()

setuptools.setup(
    name='fox-orm',
    version='0.2.4',
    author='vanutp',
    author_email='hello@vanutp.dev',
    description='Simple pydantic & databases based orm',
    url='https://hub.vanutp.dev',
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    extras_require={
        'dev': [
            'aiosqlite',
            'pylint'
        ]
    },
    python_requires='>=3.8',
)
