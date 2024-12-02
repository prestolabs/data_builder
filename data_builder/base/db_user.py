from absl import flags
from absl.flags import FLAGS
from python3.base.presto_util import load_json_file


flags.DEFINE_string(
    'db_users_json',
    '/remote/iosg/stock/data/db_config/db_users_us.json',
    '')

flags.DEFINE_bool(
    'db_readonly',
    True,
    '')

class DbUser(object):
  json_file_dict = {}

  def __init__(self):
    pass

  @staticmethod
  def get_database_name(database_name):
    return DbUser.get_admin_user(database_name)['database_name']

  @staticmethod
  def get_user(user_name):
    return DbUser.from_file(DbUser.get_db_user_file(), user_name)

  @staticmethod
  def get_db_user_file():
    return FLAGS.db_users_json

  @staticmethod
  def load_users_from_file(config_file):
    # Assumes filepath uniqueness.
    if config_file not in DbUser.json_file_dict:
      DbUser.json_file_dict[config_file] = load_json_file(config_file)
    return DbUser.json_file_dict[config_file]

  @staticmethod
  def from_file(config_file, db_user_name, generic_db_user_name=""):
    config_obj = DbUser.load_users_from_file(config_file)
    if db_user_name in config_obj:
      return config_obj[db_user_name]
    elif generic_db_user_name and generic_db_user_name in config_obj:
      return config_obj[generic_db_user_name]
    else:
      raise Exception("unknown db_user_name %s" % db_user_name)

  @staticmethod
  def get_admin_user(database_name):
    if FLAGS.db_readonly:
      return DbUser.from_file(
          DbUser.get_db_user_file(), "%s_readonly_prod" % database_name)
    else:
      return DbUser.from_file(
          DbUser.get_db_user_file(), "%s_admin_prod" % database_name)

  @staticmethod
  def get_readonly_user(database_name):
    specific_user = "%s_readonly_prod" % database_name
    generic_user = "readonly_prod"
    return DbUser.from_file(
        DbUser.get_db_user_file(), specific_user, generic_user)
