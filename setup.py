import setuptools

pipelines_dependencies = [
    'apache_beam==2.31.0', 'SQLAlchemy', 'PyMySQL', 'protobuf',
    'cloud-sql-python-connector'
]

setuptools.setup(name='streaming_examples',
                 version='0.0.2',
                 description='examples',
                 packages=setuptools.find_packages(),
                 install_requires=pipelines_dependencies)
