import setuptools

setuptools.setup(
    name='write_to_csql_examples',
    version='0.0.1',
    description='Write to csql example',
    packages= [
        'write_to_csql_cloud_sql_connector',
        'write_to_csql_cloud_sql_connector.exceptions',
        'write_to_csql_cloud_sql_connector.utils'])