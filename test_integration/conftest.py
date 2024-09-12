def pytest_addoption(parser):
    parser.addoption("--snowflake-session", action="store", default="live", help="--snowflake-session [local|live]") ##live represents Snowflake connection
