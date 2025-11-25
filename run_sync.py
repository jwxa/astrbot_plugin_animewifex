import asyncio, json, os, sys, types, logging, shutil, tempfile

# stub astrbot.api.logger
a_logger = logging.getLogger("astrbot_test")
a_logger.setLevel(logging.INFO)
a_logger.addHandler(logging.StreamHandler())
mod_api = types.ModuleType("astrbot.api")
mod_api.logger = a_logger
mod_astrbot = types.ModuleType("astrbot")
mod_astrbot.api = mod_api
sys.modules["astrbot"] = mod_astrbot
sys.modules["astrbot.api"] = mod_api

sys.path.append(os.path.abspath('.'))
from animewife_syncer import AnimeWifeSyncer
branch = "test_for_pic_sync"
cdn_prefix = ""
repo_url = "https://github.com/jwxa/astrbot_plugin_animewifex"
config = {
    "repo_sync_cron": "0 */6 * * *",
    "use_repo_cdn": False,
    "repo_cdn_prefix": cdn_prefix,
    "repo_sources": [
        {
            "type": "git download",
            "repo_url": "https://github.com/jwxa/astrbot_plugin_animewifex",
            "repo_branch": branch,
            "paths": ["*"],
        },
        {
            "type": "git repo",
            "repo_url": "https://github.com/jwxa/astrbot_plugin_animewifex.git",
            "repo_branch": branch,
            "paths": ["*"],
        },
    ],
}
defaults = {
    "repo_url": repo_url,
    "branch": branch,
    "cdn_prefix": cdn_prefix,
    "min_sync_interval": 1,
}
base_dir = tmp_dir = tempfile.gettempdir()
img_dir = os.path.join(base_dir, 'img', 'wife')
state_file = os.path.join(base_dir, 'repo_sync_state.json')
os.makedirs(img_dir, exist_ok=True)

syncer = AnimeWifeSyncer(
    config,
    img_dir=img_dir,
    state_file=state_file,
    defaults=defaults,
    repo_base_dir=base_dir,
)

async def main():
    ok = await syncer.sync_repos(force=True)
    print("sync ok:", ok)
    print("state exists:", os.path.exists(state_file))
    if os.path.exists(state_file):
        print("state:", json.load(open(state_file, "r", encoding="utf-8")))
    if os.path.isdir(img_dir):
        files = os.listdir(img_dir)
        print("image count:", len(files))
        print("sample:", files[:10])
        expected = ["2.5次元的诱惑!橘美花莉.jpg", "火影忍者!春野樱.jpg"]
        print("missing expected:", [name for name in expected if name not in files])

asyncio.run(main())
