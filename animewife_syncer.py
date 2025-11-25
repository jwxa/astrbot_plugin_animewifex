import asyncio
import contextlib
import fnmatch
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
import zipfile
from datetime import datetime
from typing import Any

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from astrbot.api import logger


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
DEFAULT_FILENAME_PATTERN = r"^[^!]+![^!]+\.[^!]+$"
RESOURCE_TYPE_GIT_DOWNLOAD = "git download"
RESOURCE_TYPE_GIT_REPO = "git repo"


def normalize_repo_paths(raw: Any):
    if not raw:
        return ["wife"]
    if isinstance(raw, str):
        cleaned = raw.strip().strip("/\\")
        return [cleaned] if cleaned else ["wife"]
    if isinstance(raw, list):
        paths = []
        for item in raw:
            if isinstance(item, str):
                cleaned = item.strip().strip("/\\")
                if cleaned:
                    paths.append(cleaned)
        return paths or ["wife"]
    return ["wife"]


def with_cdn_prefix(url: str, cdn_prefix: str, enabled: bool) -> str:
    if not enabled:
        return url
    prefix = (cdn_prefix or "").rstrip("/")
    if not prefix:
        return url
    return f"{prefix}/{url}"


class AnimeWifeSyncer:
    def __init__(
        self,
        config,
        img_dir: str,
        state_file: str,
        defaults: dict,
        repo_base_dir: str | None = None,
    ):
        self.img_dir = img_dir
        self.state_file = state_file
        self.default_repo_url = defaults.get("repo_url")
        self.default_branch = defaults.get("branch")
        self.default_cdn_prefix = defaults.get("cdn_prefix")
        self.min_sync_interval = defaults.get("min_sync_interval", 1)
        self.default_filename_pattern = (
            defaults.get("filename_pattern") or DEFAULT_FILENAME_PATTERN
        )

        sync_config: Dict = config.get("sync", {})
        self.repo_url = sync_config.get("repo_url") or self.default_repo_url
        self.repo_branch = sync_config.get("repo_branch") or self.default_branch
        self.repo_image_paths = normalize_repo_paths(sync_config.get("repo_image_paths"))
        pattern_raw = (sync_config.get("filename_pattern") or self.default_filename_pattern or "").strip()
        self.filename_pattern = re.compile(pattern_raw) if pattern_raw else None
        self.repo_sync_cron = (sync_config.get("repo_sync_cron") or "0 */6 * * *").strip()
        self.use_repo_cdn = bool(sync_config.get("use_repo_cdn"))
        self.repo_cdn_prefix = (sync_config.get("repo_cdn_prefix") or "").rstrip("/")
        self.repo_base_dir = os.path.abspath(
            repo_base_dir or os.path.dirname(os.path.abspath(state_file))
        )
        self.repo_cache_dir = os.path.join(self.repo_base_dir, "repos")
        os.makedirs(self.repo_cache_dir, exist_ok=True)
        self.repo_sources = self._build_repo_sources(config)
        self._sync_lock = asyncio.Lock()
        self._scheduler = None
        self._initial_task = None

    def _normalize_source_type(self, raw) -> str:
        if not raw:
            return RESOURCE_TYPE_GIT_DOWNLOAD
        value = str(raw).strip().lower().replace("_", " ")
        if value in {RESOURCE_TYPE_GIT_REPO, "gitrepo"}:
            return RESOURCE_TYPE_GIT_REPO
        return RESOURCE_TYPE_GIT_DOWNLOAD

    def _build_repo_sources(self, config: dict):
        extra_sources = []
        if isinstance(config, dict):
            extra_sources = (
                config.get("repo_sources")
                or config.get("resource_urls")
                or []
            )
        base_type = self._normalize_source_type(
            config.get("repo_type") if isinstance(config, dict) else None
        )
        if isinstance(extra_sources, list) and extra_sources:
            sources = []
            for item in extra_sources:
                if not isinstance(item, dict):
                    continue
                url = item.get("repo_url") or item.get("url") or self.repo_url
                if not url:
                    continue
                branch = item.get("branch") or item.get("repo_branch") or self.repo_branch
                source_type = self._normalize_source_type(
                    item.get("type") or item.get("repo_type") or base_type
                )
                paths = normalize_repo_paths(
                    item.get("paths") or item.get("repo_image_paths") or self.repo_image_paths,
                )
                sources.append(
                    {
                        "type": source_type,
                        "url": url,
                        "branch": branch,
                        "paths": paths,
                        "use_cdn": bool(item.get("use_repo_cdn", self.use_repo_cdn)),
                        "cdn_prefix": (
                            item.get("repo_cdn_prefix") or self.repo_cdn_prefix
                        ).rstrip("/"),
                    }
                )
            return sources
        return [
            {
                "type": base_type,
                "url": self.repo_url,
                "branch": self.repo_branch,
                "paths": self.repo_image_paths,
                "use_cdn": self.use_repo_cdn,
                "cdn_prefix": self.repo_cdn_prefix,
            }
        ]

    async def initialize(self):
        self._initial_task = asyncio.create_task(self.sync_repos(force=True))
        try:
            trigger = CronTrigger.from_crontab(self.repo_sync_cron)
            self._scheduler = AsyncIOScheduler()
            self._scheduler.add_job(self.sync_repos, trigger)
            self._scheduler.start()
        except Exception as exc:
            logger.warning(f"初始化图包定时任务失败: {exc}")

    async def terminate(self):
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        if self._initial_task:
            self._initial_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._initial_task
        if self._sync_lock.locked():
            self._sync_lock.release()

    async def ensure_local_images_ready(self):
        try:
            return os.listdir(self.img_dir)
        except FileNotFoundError:
            return []

    def _source_key(self, source: dict) -> str:
        return f"{source.get('type', RESOURCE_TYPE_GIT_DOWNLOAD)}:{source['url']}@{source['branch']}"

    def _human_size(self, num_bytes: int) -> str:
        if num_bytes <= 0:
            return "0 B"
        units = ["B", "KiB", "MiB", "GiB", "TiB"]
        size = float(num_bytes)
        idx = 0
        while size >= 1024 and idx < len(units) - 1:
            size /= 1024
            idx += 1
        return f"{size:.2f} {units[idx]}"

    def _format_progress_bar(self, percent: int, total_bytes: int, filename: str) -> str:
        width = 60
        filled = int(width * percent / 100)
        bar = "■" * filled + " " * (width - filled)
        size_label = self._human_size(total_bytes) if total_bytes else "unknown"
        return f"|{bar}| {percent:>3}% of {size_label}  {filename}"

    def _filename_allowed(self, filename: str) -> bool:
        if not self.filename_pattern:
            return True
        return bool(self.filename_pattern.match(filename))

    def _load_existing_hashes(self) -> set[str]:
        seen: set[str] = set()
        if not os.path.isdir(self.img_dir):
            return seen
        for name in os.listdir(self.img_dir):
            path = os.path.join(self.img_dir, name)
            if not os.path.isfile(path):
                continue
            try:
                with open(path, "rb") as f:
                    data = f.read()
            except Exception:
                continue
            seen.add(hashlib.md5(data).hexdigest())
        return seen

    async def sync_repos(self, force: bool = False):
        if not self.repo_sources:
            return False
        if self._sync_lock.locked():
            logger.warning("\u68c0\u6d4b\u5230\u6b63\u5728\u540c\u6b65\u56fe\u7247\u5305\uff0c\u8df3\u8fc7\u672c\u6b21\u540c\u6b65\uff08\u5e76\u53d1\u5feb\u901f\u5931\u8d25\uff09")
            return False
        try:
            await asyncio.wait_for(self._sync_lock.acquire(), timeout=0.01)
        except asyncio.TimeoutError:
            logger.warning("\u83b7\u53d6\u56fe\u7247\u540c\u6b65\u9501\u8d85\u65f6\uff0c\u8df3\u8fc7\u672c\u6b21\u540c\u6b65")
            return False
        try:
            state = self._load_json(self.state_file)
            state_sources = state.get("sources", {})
            staging_dir = tempfile.mkdtemp(prefix="animewifex_stage_")
            total_extracted = 0
            new_state_sources = {}
            summary = {
                "downloaded_bytes": 0,
                "sources": [],
                "skipped_pattern": 0,
                "skipped_duplicate": 0,
                "skipped_non_image": 0,
                "skipped_path": 0,
            }
            seen_hashes = self._load_existing_hashes()
            for source in self.repo_sources:
                result = await self._sync_single_repo(
                    source, staging_dir, force, state_sources, seen_hashes
                )
                total_extracted += result.get("extracted", 0)
                summary["downloaded_bytes"] += result.get("downloaded_bytes", 0)
                summary["skipped_pattern"] += result.get("skipped_pattern", 0)
                summary["skipped_duplicate"] += result.get("skipped_duplicate", 0)
                summary["skipped_non_image"] += result.get("skipped_non_image", 0)
                summary["skipped_path"] += result.get("skipped_path", 0)
                summary["sources"].append(
                    {
                        "type": source.get("type", RESOURCE_TYPE_GIT_DOWNLOAD),
                        "repo": source["url"],
                        "branch": source["branch"],
                        "extracted": result.get("extracted", 0),
                        "skipped_pattern": result.get("skipped_pattern", 0),
                        "skipped_duplicate": result.get("skipped_duplicate", 0),
                        "skipped_non_image": result.get("skipped_non_image", 0),
                        "skipped_path": result.get("skipped_path", 0),
                        "latest_commit": result.get("latest_commit"),
                        "status": result.get("status"),
                    }
                )
                key = self._source_key(source)
                prev = state_sources.get(key, {})
                new_state_sources[key] = {
                    "last_commit": result.get("latest_commit") or prev.get("last_commit"),
                    "repo": source["url"],
                    "branch": source["branch"],
                }
            if total_extracted:
                self._merge_and_replace_local_images(staging_dir)
            else:
                shutil.rmtree(staging_dir, ignore_errors=True)
            state.update(
                {
                    "sources": new_state_sources,
                    "last_synced": datetime.utcnow().isoformat() + "Z",
                    "last_extracted": total_extracted,
                }
            )
            self._save_json(self.state_file, state)
            for item in summary["sources"]:
                logger.info(
                    f"[{item.get('type')}] {item['repo']}@{item['branch']} -> 提取 {item['extracted']}，跳过：正则 {item['skipped_pattern']} / 重复 {item['skipped_duplicate']} / 路径 {item['skipped_path']} / 非图片 {item['skipped_non_image']}；状态 {item.get('status') or '完成'}"
                )
            total_skipped = (
                summary["skipped_pattern"]
                + summary["skipped_duplicate"]
                + summary["skipped_non_image"]
                + summary["skipped_path"]
            )
            logger.info(
                f"图片资源同步报告：下载 {self._human_size(summary['downloaded_bytes'])}，同步 {total_extracted} 张，跳过 {total_skipped} 张（正则 {summary['skipped_pattern']}，重复 {summary['skipped_duplicate']}，路径 {summary['skipped_path']}，非图片 {summary['skipped_non_image']}），源 {len(self.repo_sources)} 个"
            )
            if total_extracted:
                logger.info(f"图片资源同步完成，本次更新 {total_extracted} 张")
            return bool(total_extracted)
        finally:
            if self._sync_lock.locked():
                self._sync_lock.release()

    async def _sync_single_repo(
        self,
        source: dict,
        staging_dir: str,
        force: bool,
        state_sources: dict,
        seen_hashes: set[str],
    ) -> dict:
        source_type = source.get("type") or RESOURCE_TYPE_GIT_DOWNLOAD
        if source_type == RESOURCE_TYPE_GIT_REPO:
            return await self._sync_git_repo(
                source, staging_dir, force, state_sources, seen_hashes
            )
        return await self._sync_git_download(
            source, staging_dir, force, state_sources, seen_hashes
        )

    async def _sync_git_download(
        self,
        source: dict,
        staging_dir: str,
        force: bool,
        state_sources: dict,
        seen_hashes: set[str],
    ) -> dict:
        result = {
            "extracted": 0,
            "skipped_pattern": 0,
            "skipped_duplicate": 0,
            "skipped_non_image": 0,
            "skipped_path": 0,
            "latest_commit": None,
            "downloaded_bytes": 0,
            "status": "",
        }
        try:
            owner, repo, branch = self._parse_repo_url(source["url"], source["branch"])
        except ValueError as exc:
            logger.warning(f"解析图片仓库地址失败: {exc}")
            result["status"] = "解析仓库失败"
            return result
        timeout = aiohttp.ClientTimeout(total=600)
        archive_url = self._build_archive_url(
            owner, repo, branch, source["cdn_prefix"], source["use_cdn"]
        )
        key = self._source_key(source)
        prev = state_sources.get(key, {})
        archive_name = f"{repo}-{branch}.zip"
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                result["latest_commit"] = await self._fetch_latest_commit(
                    session, owner, repo, branch
                )
                if (
                    not force
                    and result["latest_commit"]
                    and prev.get("last_commit") == result["latest_commit"]
                    and os.path.isdir(self.img_dir)
                    and os.listdir(self.img_dir)
                ):
                    result["status"] = "已是最新"
                    return result
                zip_path, downloaded_bytes, archive_label = await self._download_archive(
                    session, archive_url, archive_name
                )
                result["downloaded_bytes"] = downloaded_bytes
                archive_name = archive_label
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"获取图片仓库失败: {exc}")
            result["status"] = "下载失败"
            return result
        stats = self._extract_images(
            zip_path, staging_dir, source["paths"], seen_hashes
        )
        try:
            os.remove(zip_path)
        except Exception:
            pass
        result.update(stats)
        result["status"] = result["status"] or "完成"
        return result

    async def _sync_git_repo(
        self,
        source: dict,
        staging_dir: str,
        force: bool,
        state_sources: dict,
        seen_hashes: set[str],
    ) -> dict:
        result = {
            "extracted": 0,
            "skipped_pattern": 0,
            "skipped_duplicate": 0,
            "skipped_non_image": 0,
            "skipped_path": 0,
            "latest_commit": None,
            "downloaded_bytes": 0,
            "status": "",
        }
        try:
            owner, repo, branch = self._parse_repo_url(source["url"], source["branch"])
        except ValueError as exc:
            logger.warning(f"解析图片仓库地址失败: {exc}")
            result["status"] = "解析仓库失败"
            return result
        key = self._source_key(source)
        prev = state_sources.get(key, {})
        repo_dir = self._build_repo_dir(owner, repo, branch)
        try:
            await self._ensure_repo_initialized(repo_dir, source["url"], branch)
            logger.info(f"[git repo] 准备同步 {source['url']} 到 {repo_dir}")
            await self._run_git_command(
                ["-C", repo_dir, "remote", "set-url", "origin", source["url"]],
            )
            logger.info(f"[git repo] fetch origin for {repo_dir}")
            await self._run_git_command(
                ["-C", repo_dir, "fetch", "origin"],
                log_progress=True,
            )
            local_head_before = await self._git_rev_parse(repo_dir, branch) or await self._git_rev_parse(repo_dir, "HEAD")
            await self._run_git_command(["-C", repo_dir, "checkout", branch])
            remote_head = await self._git_rev_parse(repo_dir, f"origin/{branch}")
            if remote_head and prev.get("last_commit") == remote_head and local_head_before == remote_head:
                result["latest_commit"] = remote_head
                result["status"] = "已是最新"
                logger.info(f"[git repo] {repo_dir} 已是最新，跳过合并")
                return result
            logger.info(f"[git repo] pull origin/{branch} into {repo_dir}")
            await self._run_git_command(
                ["-C", repo_dir, "pull", "--ff-only", "origin", branch],
                log_progress=True,
            )
            head_commit = await self._git_rev_parse(repo_dir, "HEAD")
            result["latest_commit"] = head_commit or remote_head
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"同步 Git 仓库失败: {exc}")
            result["status"] = "拉取失败"
            return result
        stats = self._collect_images_from_repo(
            repo_dir, staging_dir, source["paths"], seen_hashes
        )
        result.update(stats)
        result["status"] = result["status"] or "完成"
        return result

    async def _ensure_repo_initialized(self, repo_dir: str, repo_url: str, branch: str):
        git_dir = os.path.join(repo_dir, ".git")
        config_path = os.path.join(git_dir, "config")
        if os.path.isdir(git_dir) and os.path.isfile(config_path):
            return
        if os.path.exists(repo_dir):
            try:
                def _onerror(func, path, exc_info):
                    try:
                        os.chmod(path, 0o700)
                        func(path)
                    except Exception:
                        pass

                shutil.rmtree(repo_dir, onerror=_onerror)
            except Exception as exc:
                raise RuntimeError(f"无法清理旧仓库目录：{repo_dir}，原因：{exc}") from exc
        logger.info(f"[git repo] clone {repo_url} -> {repo_dir} ({branch})")
        await self._run_git_command(
            [
                "clone",
                "--branch",
                branch,
                "--single-branch",
                "--depth",
                "1",
                repo_url,
                repo_dir,
            ],
            log_progress=True,
        )

    async def _git_rev_parse(self, repo_dir: str, ref: str) -> str | None:
        try:
            output = await self._run_git_command(
                ["-C", repo_dir, "rev-parse", ref]
            )
            return output.strip()
        except Exception:
            return None

    async def _run_git_command(
        self,
        args: list[str],
        cwd: str | None = None,
        log_progress: bool = False,
    ) -> str:
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_CONFIG_GLOBAL"] = os.devnull
        env["GIT_CONFIG_SYSTEM"] = os.devnull
        proc = await asyncio.create_subprocess_exec(
            "git",
            *args,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        if log_progress:
            lines: list[str] = []

            async def _drain(stream):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", "ignore").rstrip()
                    lines.append(text)
                    if text:
                        logger.info(f"[git] {text}")

            await asyncio.gather(_drain(proc.stdout), _drain(proc.stderr))
            await proc.wait()
            output = "\n".join(lines)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"git {' '.join(args)} 失败：{output}".strip()
                )
            return output
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            stderr_text = stderr.decode("utf-8", "ignore")
            stdout_text = stdout.decode("utf-8", "ignore")
            raise RuntimeError(
                f"git {' '.join(args)} 失败：{stderr_text or stdout_text}".strip()
            )
        return stdout.decode("utf-8", "ignore")

    def _build_repo_dir(self, owner: str, repo: str, branch: str) -> str:
        safe_owner = re.sub(r"[^a-zA-Z0-9._-]", "-", owner)
        safe_repo = re.sub(r"[^a-zA-Z0-9._-]", "-", repo)
        safe_branch = re.sub(r"[^a-zA-Z0-9._-]", "-", branch or "main")
        return os.path.join(self.repo_cache_dir, f"{safe_owner}-{safe_repo}-{safe_branch}")

    def _collect_images_from_repo(
        self,
        repo_dir: str,
        staging_dir: str,
        patterns: list[str],
        seen_hashes: set[str],
    ) -> dict:
        stats = {
            "extracted": 0,
            "skipped_pattern": 0,
            "skipped_duplicate": 0,
            "skipped_non_image": 0,
            "skipped_path": 0,
        }
        for root, _, files in os.walk(repo_dir):
            parts = root.split(os.sep)
            if ".git" in parts:
                continue
            for name in files:
                rel_path = os.path.relpath(os.path.join(root, name), repo_dir)
                normalized = rel_path.replace("\\", "/")
                if not self._match_repo_path(normalized, patterns):
                    stats["skipped_path"] += 1
                    continue
                ext = os.path.splitext(name)[1].lower()
                if ext not in IMAGE_EXTS:
                    stats["skipped_non_image"] += 1
                    continue
                if not self._filename_allowed(name):
                    stats["skipped_pattern"] += 1
                    continue
                try:
                    with open(os.path.join(root, name), "rb") as f:
                        data = f.read()
                except Exception as exc:
                    logger.warning(f"读取文件失败 {rel_path}: {exc}")
                    continue
                file_hash = hashlib.md5(data).hexdigest()
                if file_hash in seen_hashes:
                    stats["skipped_duplicate"] += 1
                    logger.info(f"跳过重复文件：{name}（hash {file_hash[:8]}…）")
                    continue
                seen_hashes.add(file_hash)
                os.makedirs(staging_dir, exist_ok=True)
                dest_path = os.path.join(staging_dir, name)
                with open(dest_path, "wb") as dst:
                    dst.write(data)
                stats["extracted"] += 1
        return stats

    def _merge_and_replace_local_images(self, staging_dir: str):
        """保留旧图：先将旧目录中的文件拷贝到 staging（不覆盖），再整体替换。"""
        backup_dir = self.img_dir + ".bak"
        try:
            if os.path.isdir(self.img_dir):
                for name in os.listdir(self.img_dir):
                    src_path = os.path.join(self.img_dir, name)
                    dst_path = os.path.join(staging_dir, name)
                    if not os.path.isfile(src_path):
                        continue
                    if os.path.exists(dst_path):
                        continue  # staging 中已有同名文件，优先新内容
                    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                    shutil.copy2(src_path, dst_path)
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir, ignore_errors=True)
            if os.path.exists(self.img_dir):
                shutil.move(self.img_dir, backup_dir)
            shutil.move(staging_dir, self.img_dir)
            shutil.rmtree(backup_dir, ignore_errors=True)
        except Exception as exc:
            logger.warning(f"替换本地图包失败: {exc}")
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _build_archive_url(
        self,
        owner: str,
        repo: str,
        branch: str,
        cdn_prefix: str,
        use_cdn: bool,
    ) -> str:
        archive = f"https://github.com/{owner}/{repo}/archive/refs/heads/{branch}.zip"
        prefix = cdn_prefix or self.default_cdn_prefix or ""
        return with_cdn_prefix(archive, prefix, use_cdn)

    async def _fetch_latest_commit(
        self, session: aiohttp.ClientSession, owner: str, repo: str, branch: str
    ) -> str | None:
        api_url = f"https://api.github.com/repos/{owner}/{repo}/commits/{branch}"
        try:
            async with session.get(api_url, timeout=15) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"获取最新提交信息失败: {exc}")
            return None
        if isinstance(data, dict):
            return data.get("sha")
        if isinstance(data, list) and data:
            return data[0].get("sha")
        return None

    async def _download_archive(
        self, session: aiohttp.ClientSession, url: str, archive_name: str | None = None
    ) -> tuple[str, int, str]:
        fd, tmp_path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        downloaded = 0
        label = archive_name or os.path.basename(url.split("?", 1)[0]) or "archive.zip"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                total_size = int(resp.headers.get("Content-Length") or 0)
                if total_size:
                    logger.info(f"开始下载 {label} (大小: {self._human_size(total_size)})")
                else:
                    logger.info(f"开始下载 {label} (大小未知)")
                last_percent = -5
                last_bytes_logged = 0
                last_time = time.monotonic()
                with open(tmp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size:
                            percent = min(100, int(downloaded * 100 / total_size))
                            now = time.monotonic()
                            if percent == 100 or percent - last_percent >= 5 or now - last_time >= 5:
                                logger.info(self._format_progress_bar(percent, total_size, label))
                                last_percent = percent
                                last_time = now
                        else:
                            now = time.monotonic()
                            if (
                                downloaded - last_bytes_logged >= 1024 * 1024
                                or now - last_time >= 5
                            ):
                                logger.info(
                                    f"{label} 已下载 {self._human_size(downloaded)}（总大小未知）"
                                )
                                last_bytes_logged = downloaded
                                last_time = now
                if total_size:
                    logger.info(self._format_progress_bar(100, total_size, label))
                else:
                    logger.info(f"{label} 下载完成，合计 {self._human_size(downloaded)}")
            return tmp_path, downloaded, label
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def _match_repo_path(self, relative_path: str, patterns: list[str]) -> bool:
        normalized = relative_path.replace("\\", "/")
        for pattern in patterns:
            cleaned = pattern.strip("/\\")
            if not cleaned:
                continue
            if normalized.startswith(f"{cleaned}/") or normalized == cleaned:
                return True
            if fnmatch.fnmatch(normalized, cleaned):
                return True
        return False

    def _extract_images(
        self, zip_path: str, staging_dir: str, patterns: list[str], seen_hashes: set[str]
    ) -> dict:
        stats = {
            "extracted": 0,
            "skipped_pattern": 0,
            "skipped_duplicate": 0,
            "skipped_non_image": 0,
            "skipped_path": 0,
        }
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            root_prefix = ""
            if names:
                first = names[0].replace("\\", "/")
                if "/" in first:
                    root_prefix = first.split("/", 1)[0] + "/"
            for name in names:
                if name.endswith("/"):
                    continue
                normalized = name.replace("\\", "/")
                relative = (
                    normalized[len(root_prefix) :]
                    if normalized.startswith(root_prefix)
                    else normalized
                )
                if not self._match_repo_path(relative, patterns):
                    stats["skipped_path"] += 1
                    continue
                ext = os.path.splitext(relative)[1].lower()
                if ext not in IMAGE_EXTS:
                    stats["skipped_non_image"] += 1
                    continue
                filename = os.path.basename(relative)
                if not self._filename_allowed(filename):
                    stats["skipped_pattern"] += 1
                    continue
                with zf.open(name) as src:
                    data = src.read()
                file_hash = hashlib.md5(data).hexdigest()
                if file_hash in seen_hashes:
                    stats["skipped_duplicate"] += 1
                    logger.info(f"跳过重复文件：{filename}（hash {file_hash[:8]}…）")
                    continue
                seen_hashes.add(file_hash)
                os.makedirs(staging_dir, exist_ok=True)
                dest_path = os.path.join(staging_dir, filename)
                with open(dest_path, "wb") as dst:
                    dst.write(data)
                stats["extracted"] += 1
        return stats

    def _parse_repo_url(self, url: str, fallback_branch: str | None):
        cleaned = (url or self.default_repo_url).rstrip("/")
        pattern = r"^https://github\.com/([^/]+)/([^/]+?)(?:\.git)?(?:/tree/([^/]+))?$"
        match = re.match(pattern, cleaned)
        if not match:
            raise ValueError("无效的 GitHub 仓库地址")
        owner, repo, branch = match.group(1), match.group(2), match.group(3)
        repo = repo.removesuffix(".git")
        branch = fallback_branch or branch or self.default_branch or "main"
        return owner, repo, branch

    def _load_json(self, path: str):
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}

    def _save_json(self, path: str, data):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
