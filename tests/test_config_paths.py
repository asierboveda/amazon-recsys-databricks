from amazon_recsys import config


def test_root_directory_name():
    assert config.ROOT_DIR.name == "amazon-recsys-databricks"
