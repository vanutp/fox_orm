import setuptools

setuptools.setup(
    name='fox-orm',
    version='0.3.5.3',
    author='vanutp',
    author_email='hello@vanutp.dev',
    description='Simple pydantic & databases based orm',
    url='https://hub.vanutp.dev',
    packages=setuptools.find_packages(),
    install_requires=[
        'databases~=0.5.2',
        'pydantic~=1.8.2',
    ],
    python_requires='>=3.8',
)
