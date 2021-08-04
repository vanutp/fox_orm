import setuptools

setuptools.setup(
    name='fox-orm',
    version='0.3.0.1',
    author='vanutp',
    author_email='hello@vanutp.dev',
    description='Simple pydantic & databases based orm',
    url='https://hub.vanutp.dev',
    packages=setuptools.find_packages(),
    install_requires=[
        'databases~=0.4.3',
        'pydantic~=1.8.2',
        'SQLAlchemy~=1.3.24',
    ],
    python_requires='>=3.8',
)
