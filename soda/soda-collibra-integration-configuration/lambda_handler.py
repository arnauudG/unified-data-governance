# lambda_handler.py (excerpt)
import os, json, yaml, boto3, inspect

s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")

def _read_s3_yaml_to_dict(s3_uri: str):
    assert s3_uri.startswith("s3://"), "CONFIG_S3_URI must start with s3://"
    _, _, path = s3_uri.partition("s3://")
    bucket, _, key = path.partition("/")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return yaml.safe_load(obj["Body"].read())

def _merge_secrets(cfg):
    soda_arn = os.environ.get("SODA_SECRET_ARN")
    collibra_arn = os.environ.get("COLLIBRA_SECRET_ARN")
    if soda_arn:
        s = sm.get_secret_value(SecretId=soda_arn)
        soda = json.loads(s["SecretString"])
        cfg.setdefault("soda", {}).update({
            "api_key_id": soda["api_key_id"],
            "api_key_secret": soda["api_key_secret"],
        })
    if collibra_arn:
        s = sm.get_secret_value(SecretId=collibra_arn)
        col = json.loads(s["SecretString"])
        cfg.setdefault("collibra", {}).update({
            "username": col["username"],
            "password": col["password"],
        })
    return cfg

def handler(event, context):
    # 1) Load + merge config
    base_cfg = _read_s3_yaml_to_dict(os.environ["CONFIG_S3_URI"])
    cfg = _merge_secrets(base_cfg)

    # 2) Write merged config to /tmp
    merged_path = "/tmp/config.yaml"
    with open(merged_path, "w") as f:
        yaml.safe_dump(cfg, f)

    # 3) Call your CLI, supporting both main() and main(argv)
    import main as cli
    argv = ["--config", merged_path]
    if os.environ.get("DEBUG") == "1":
        argv.append("--debug")

    if hasattr(cli, "main"):
        sig = inspect.signature(cli.main)
        if len(sig.parameters) == 0:
            # emulate CLI: set sys.argv then call main()
            import sys
            sys.argv = ["main.py"] + argv
            return cli.main() or {"status": "ok"}
        else:
            return cli.main(argv) or {"status": "ok"}
    elif hasattr(cli, "run"):
        # fallback if you expose a run() API
        return cli.run(config_path=merged_path, debug=os.environ.get("DEBUG") == "1")

    raise RuntimeError("main.py must expose main() or main(argv) or run(...)")
