#!/usr/local/bin/python
import argparse
import os
import psycopg2

from alembic.config import Config
from alembic import command
from alembic.util.exc import CommandError

from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_store_obj import ContentStoreObject
import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ContentStoreManager(object):
    """
    Class to manage database initialization and migration
    These commands can be run inside the conductor container:
    `python3 -m timpani.content_store.content_store_manager setup`

    or from outside the container:

    `docker compose run conductor content_store setup`

    Alembic migrations are are applied via `content_store migrate`

    New migration templates can be created with something like from (from inside the container)
    `python3 -m alembic -c timpani/content_store/alembic.ini revision --autogenerate  -m 'create initial table definitions'`

    """

    # TODO: This needs to support dependency injection of alternate database like sqlite

    cfg = TimpaniAppCfg()
    alembic_cfg = Config("timpani/content_store/alembic.ini")
    # create a content store
    store = ContentStore()

    def _get_root_admin_connection(self, db="postgres"):
        """
        get the *database* root users and credentials needed login to create admin users
        (these env variables are normally set in environment_variable.env) and return
        corresponding connection
        """
        usr = os.environ.get("POSTGRES_USER")
        pwd = os.environ.get("POSTGRES_PASSWORD")
        host = os.environ.get("CONTENT_STORE_RW_ENDPOINT")
        port = 5432
        if len(host.split(":")) > 1:
            # grab the port from the host string
            port = host.split(":")[1]
            host = host.split(":")[0]

        logging.debug(f"Connecting to {db} at {host}:{port} as root admin {usr}")
        connection = psycopg2.connect(
            database=db,
            user=usr,
            password=pwd,
            host=host,
            port=port,
        )
        return connection

    def setup_admin_and_content_store(self):
        """
        Create the admin role that can modify database, this role will then create other roles, tables, etc.

        This should only be run for the initial configuration of new database.
        For AWS RDS (qa/live):
        This has to be run as the aws root user, talking to postgres system db.  Password can be found in aws rds console for the cluster
        psql --host=timpani-content-store-qa.cluster-cywyawvr8n7c.eu-west-1.rds.amazonaws.com --username=root  --password postgres
        For local dev, this is the POSTGRES_USER and POSTGRES_PASSWORD in the environment_variable.env:
         psql --host=localhost --username=postgres  (password is probably 'postgres')

        """
        # TODO: this can still leak passwords into logs if we have errors running SQL statements, how to fix?
        # NOTE: if the already exists, this will fail with psycopg2.errors.DuplicateObject: role "timpani_admin" already exists
        create_role_sql = f"""
        CREATE ROLE {self.cfg.content_store_admin_user} with password '{self.cfg.content_store_admin_pwd}' LOGIN CREATEROLE;
        """
        create_db_sql = f"CREATE DATABASE {self.cfg.content_store_db};"
        grant_sql = f"""
        GRANT CONNECT ON DATABASE {self.cfg.content_store_db} to {self.cfg.content_store_admin_user};
        GRANT ALL PRIVILEGES ON DATABASE {self.cfg.content_store_db}  TO {self.cfg.content_store_admin_user} WITH GRANT OPTION;
        """

        if self.cfg.deploy_env_label in ["test", "dev"]:
            connection = self._get_root_admin_connection()
            connection.autocommit = True
            try:
                logging.info(
                    f"Creating role {self.cfg.content_store_admin_user} and db {self.cfg.content_store_db} and granting access"
                )
                with connection.cursor() as cursor:
                    cursor.execute(create_role_sql)
                    cursor.execute(create_db_sql)
                    cursor.execute(grant_sql)
            except psycopg2.errors.DuplicateObject as e:
                # expecting psycopg2.errors.DuplicateObject: role "timpani_admin" already exists
                logging.warning(
                    f" setup not run because {e} To recreate database, run 'content_store destroy' first"
                )
        else:
            # TODO: get access to RDS cluster database via iam role
            # TODO: determine if database setup needs to be run
            # meanwhile, let's not write the admin password into logs
            create_role_sql = create_role_sql.replace(
                self.cfg.content_store_admin_pwd, "<content_store_admin_pwd>"
            )
            logging.info("Printing database setup instructions to console")
            print(
                "Password can be found in AWS RDS console for the cluster, or in the environment_variables for local dev"
            )
            print(
                "To setup the database, login to the database as the 'root admin' user and run the following sql:"
            )
            print(create_role_sql)
            print(create_db_sql)
            print(grant_sql)

        # determine which migration state we are in
        command.current(self.alembic_cfg)
        # report if migrations need to be run
        try:
            command.check(self.alembic_cfg)
        except CommandError as e:
            msg = f"content_store db migration check error: {e} Please run database migrations"
            print(msg)
            logging.error(msg)
            # if in dev or test, try to run the migrations
            if self.cfg.deploy_env_label in ["test", "dev"]:
                self.migrate_content_store()

    def init_content_store(self):
        """
        NOTE: THIS HAS BEEN REPLACED WITH ALEMBIC MIGRATION
        Calls the ORM's 'create_all' function to initialize tables
        and update with any new columns. Should be safe to run on
        tables with existing data (but potentially dropping columns)
        """

        # connect to the database using

        engine = self.store.init_db_engine()
        # define all the new tables

        logging.info("\nInitializing content_store tables\n")
        # Since all of the objects currently decsend from ContentStoreObject
        # asking the ORM to create_all from the meta data seems to initialize everying
        ContentStoreObject.metadata.create_all(bind=engine)

    def delete_content_store(self):
        """
        Deletes all the content from tables, but leaves table structures intact.
        To used to reset the database for testing?
        TRUNCATE TABLE table_name RESTART IDENTITY CASCADE;
        """
        raise NotImplementedError

        logging.info("\nDELETING ALL CONTENT and resetting content_store tables\n")

    def destroy_content_store(self):
        """
        Destroy the admin users and erase the database, resets to the state before init content_store
        """
        # these need to be run as the database admin
        # the DROP DATABASE must be run from the 'postgres DB
        # because cannot drop the db we are logged into
        connection = self._get_root_admin_connection(db="postgres")
        connection.autocommit = True

        # run database migrations backwards to remove objects
        # command.downgrade(self.alembic_cfg, "base")

        with connection.cursor() as cursor:
            cursor.execute(
                f"DROP DATABASE IF EXISTS {self.cfg.content_store_db} WITH (FORCE);"
            )
            cursor.execute(f"DROP ROLE IF EXISTS {self.cfg.content_store_user};")
        # these have to be seperate transactions
        with connection.cursor() as cursor:
            cursor.execute(f"DROP ROLE {self.cfg.content_store_admin_user};")

        print(f"destroyed {self.cfg.content_store_db}")

    def migrate_content_store(self):
        """
        Calls the alembic migration scripts to build users, table definitions, indexes
        and any subsequent migrations.
        TODO: accept additional argument to specify specific migration other than 'head'
        """

        print(f"reading alembic config from file '{self.alembic_cfg.config_file_name}'")
        # TODO: accept arbitrary target tags?
        print(
            "Running Alembic upgrade command to apply most recent database migrations"
        )
        command.upgrade(self.alembic_cfg, "head")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TimpaniContentStoreManager",
        description=" 'setup' - initialize database tables according to ORM table definitions and alembic migrations",
        epilog="more details at https://github.com/meedan/timpani#readme",
    )
    parser.add_argument(
        "command",
        metavar="<command> [setup, migrate, destroy]",
        help="the database management command to run: 'setup', 'migrate', 'destroy'",
    )
    args = parser.parse_args()
    mgr = ContentStoreManager()
    if args.command == "init":
        logging.warning(
            "init is deprecated (except for ci testing), use the migration command to run alembic migrations"
        )
        mgr.init_content_store()
    elif args.command == "setup":
        mgr.setup_admin_and_content_store()
    elif args.command == "migrate":
        mgr.migrate_content_store()
    elif args.command == "delete":
        print(
            f"Are you sure you want to DELETE ALL DATA AND TABLES in {mgr.cfg.deploy_env_label}? (yes/no)"
        )
        choice = input()
        if choice.lower() == "yes":
            mgr.delete_content_store()
        else:
            print("Database delete canceled.")
    elif args.command == "destroy":
        print(
            f"Are you sure you want to DESTROY the content_store database in {mgr.cfg.deploy_env_label}? (yes/no)"
        )
        choice = input()
        if choice.lower() == "yes":
            mgr.destroy_content_store()
        else:
            print("Database destroy canceled.")
    else:
        print(f"unknown content store command {args.command}")
