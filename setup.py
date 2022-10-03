from setuptools import setup, find_packages

setup(
    name='IFRC_NS_Data',
    version='1.0.0',
    description='Load, clean, and transform data on IFRC National Societies and countries.',
    packages=find_packages(),
    include_package_data=True,
)
