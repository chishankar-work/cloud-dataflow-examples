import logging

class CloudSqlException(Exception):
    """A custom Cloud SQL Exception for catching insert errors

    Args:
        Exception (CloudSqlException): Insert Into exceptions
    """
    pass

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)