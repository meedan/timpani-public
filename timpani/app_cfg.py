import os

# from importlib.metadata import version


class TimpaniAppCfg(object):
    """
    This is where values that are specific to a deployment should live.
    Some of them are read from the deploy environment. Values for
    local dev environment are in environment_variables.env file.
    """

    app = "timpani"

    # TODO: this doesn't work in CI for some reason
    # timpani_version = version("timpani")

    # --- aws config
    aws_region = os.environ.get("AWS_REGION", "eu-west-1")
    # NOTE: corresponding aws profile_name entries must exist in the .aws/config file
    # aws_role_arn = "arn:aws:iam::848416313321:role/timpani-task-execution-role"
    aws_profile_name = os.environ.get("TIMPANI_AWS_PROFILE_NAME")  # "timpani-localdev"

    timpani_auth_mode = os.environ.get(
        "TIMPANI_AUTH_MODE", "meedan_check"
    )  # or None to disable auth

    # --- environment modes
    valid_env_label = [
        "local",  # runing in containers, unsually local laptop
        "dev",  # a development branch running somewhere
        "test",  # CI test environment (github actions)
        "qa",  # running in QA cloud env
        "live",  # running in production cloud env
    ]
    deploy_env_label = os.environ.get("DEPLOY_ENV", "local")
    assert (
        deploy_env_label in valid_env_label
    ), f"value of environment variable DEPLOY_ENV '{deploy_env_label}' is not recognized"

    valid_app_env = [
        "development",  # use development config settings and service endpoints
        "production",  # us production settings and endpoints
    ]
    app_env = os.environ.get("APP_ENV", "development")
    assert (
        app_env in valid_app_env
    ), f"value of environment variable APP_ENV value '{app_env}' is not recognized"

    sentry_sdk_dsn = os.environ.get("SENTRY_SDK_DSN")
    telemetery_api_key = os.environ.get("HONEYCOMB_API_KEY")
    # how often it should phone home to report data in milliseconds
    metrics_reporting_interval = os.environ.get("METRICS_REPORTING_INTERVAL", 10000)

    log_level = os.environ.get("LOG_LEVEL")

    # minio vs s3.amazonaws.com vs debug
    s3_store_location = os.environ.get("S3_STORE_LOCATION", "minio:9002")
    # updated depending on env so services know what to talk to
    timpani_conductor_api_endpoint = os.environ.get(
        "TIMPANI_CONDUCTOR_API_ENDPOINT", f"http://timpani-conductor.{deploy_env_label}"
    )
    timpani_trend_viewer_endpoint = os.environ.get(
        "TIMPANI_TREND_VIEWER_ENDPOINT",
        f"http://timpani-trend-viewer.{deploy_env_label}",
    )
    alegre_api_endpoint = os.environ.get(
        "ALEGRE_API_ENDPOINT", f"http://alegre.{deploy_env_label}"
    )
    check_graphql_base_url = os.environ.get(
        "CHECK_GRAPHQL_BASE_URL"  # e.g. https://qa-check-api.checkmedia.org
    )
    check_login_page_url = os.environ.get(
        "CHECK_LOGIN_PAGE_URL"  # e.g. https://qa.checkmedia.org
    )

    classycat_api_endpoint = os.environ.get("CLASSYCAT_API_ENDPOINT")

    # http://live-presto.live-ecs:8000
    # http://qa-presto.qa-ecs:8000
    presto_endpoint = os.environ.get(
        "PRESTO_ENDPOINT",
        f"http://presto.{deploy_env_label}:80",
    )

    # these are dev credentials, need to match with docker compose.yml
    minio_user = os.environ.get("MINIO_ROOT_USER")
    minio_password = os.environ.get("MINIO_ROOT_PASSWORD")

    # postgres content store credentials

    # base connection string for the readonly endpoint for the content store db
    content_store_db_ro_endpoint = os.environ.get("CONTENT_STORE_RO_ENDPOINT")
    # base connection string for the READ+WRITE endpoint for the content store db
    content_store_db_rw_endpoint = os.environ.get("CONTENT_STORE_RW_ENDPOINT")

    content_store_admin_user = os.environ.get("CONTENT_STORE_ADMIN_USER")
    content_store_admin_pwd = os.environ.get("CONTENT_STORE_ADMIN_PWD")

    content_store_user = os.environ.get("CONTENT_STORE_USER")
    content_store_pwd = os.environ.get("CONTENT_STORE_PWD")
    content_store_db = os.environ.get("CONTENT_STORE_DB")
