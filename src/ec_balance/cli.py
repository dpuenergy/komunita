# SPDX-License-Identifier: AGPL-3.0-or-later
import sys
import click
from .utils.config import load_yaml, kv_to_argv

def _forward_to(module_main, extra_args, passthrough_args):
    old_argv = sys.argv[:]
    try:
        sys.argv = [sys.argv[0]] + list(extra_args) + list(passthrough_args)
        return module_main()
    finally:
        sys.argv = old_argv

@click.group()
@click.version_option(package_name="ec-balance")
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="YAML s defaulty: sekce 'global' + sekce podle kroku (step1, step2, ...).",
)
@click.pass_context
def main(ctx, config):
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config

def _subcmd(name, module_path):
    @main.command(name, context_settings=dict(ignore_unknown_options=True, allow_interspersed_args=False))
    @click.argument("args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def _runner(ctx, args):
        cfg = load_yaml(ctx.obj.get("config_path"))
        extra = kv_to_argv(cfg.get("global"), cfg.get(name))
        mod = __import__(module_path, fromlist=["main"])
        _forward_to(mod.main, extra, args)
    return _runner

_subcmd("step1", "ec_balance.pipeline.step1_wide_to_long")
_subcmd("step2", "ec_balance.pipeline.step2_local_pv")
_subcmd("step3", "ec_balance.pipeline.step3_sharing")
_subcmd("step4a", "ec_balance.pipeline.step4a_batt_local_byhour")
_subcmd("step4b-econ", "ec_balance.pipeline.step4b_batt_econ")
_subcmd("step5", "ec_balance.pipeline.step5_batt_central")
_subcmd("step5a", "ec_balance.pipeline.step5a_batt_central_byhour")
_subcmd("step6", "ec_balance.pipeline.step6_excel_scenarios")
_subcmd("check", "ec_balance.utils.check")
_subcmd("doctor", "ec_balance.utils.doctor")

if __name__ == "__main__":
    main()
