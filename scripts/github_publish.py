# -*- coding: utf-8 -*-
"""GitHub 发布：创建公开仓库 → dulwich 推送 → v1.3.3 Release 附 fpk。

专家复审修正（2026-10-02）：
- push 的进度输出会把含 token 的 remote URL 打到 stdout（已实际泄漏一次，
  见会话日志）——现重定向到内存流丢弃，并在 finally 清除 token 引用；
- gh repo create / release create 增加幂等预检（已存在则跳过），脚本可安全重跑。
"""
import io
import subprocess

from dulwich import porcelain

PUB = r"D:\ShaoZY\Documents\fnmusic-flow-publish"
USER = "danbanche-byte"
REPO = "fnmusic-flow"
FPK = r"C:\Users\ShaoZY\Documents\Codex\2026-09-28\wo\outputs\fnmusic-flow-community-1.3.3.fpk"


def gh(*args):
    result = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8", errors="ignore")
    if result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:2])} 失败: {result.stderr[:300]}")
    return result.stdout.strip()


def main():
    # 1) 创建公开仓库（幂等：已存在则跳过）
    view = subprocess.run(["gh", "repo", "view", f"{USER}/{REPO}", "--json", "name"],
                          capture_output=True, text=True, encoding="utf-8", errors="ignore")
    if view.returncode == 0:
        print("1) 仓库已存在，跳过创建")
    else:
        out = gh("repo", "create", REPO, "--public",
                 "--description", "FnMusic Flow - 双平台歌单搜索/下载/飞牛音乐推送工作台（fnOS 社区版，不内置音源）")
        print("1) repo create:", (out or "created").splitlines()[-1])

    # 2) 推送（HTTPS + gh token；输出重定向丢弃，防止 token 随 remote URL 泄漏到日志）
    token = gh("auth", "token")
    remote = f"https://{USER}:{token}@github.com/{USER}/{REPO}.git"
    try:
        porcelain.push(PUB, remote, b"refs/heads/master:refs/heads/main",
                       outstream=io.BytesIO(), errstream=io.BytesIO())
        print("2) push 完成: master -> main（输出已脱敏）")
    finally:
        token = None
        remote = None

    # 3) Release v1.3.3 附 fpk（幂等：已存在则跳过附件上传）
    rel = subprocess.run(["gh", "release", "view", "v1.3.3", "--repo", f"{USER}/{REPO}"],
                         capture_output=True, text=True, encoding="utf-8", errors="ignore")
    if rel.returncode == 0:
        print("3) Release v1.3.3 已存在，跳过")
    else:
        out = gh("release", "create", "v1.3.3", FPK,
                 "--repo", f"{USER}/{REPO}",
                 "--title", "v1.3.3",
                 "--notes", "首个公开发布版本。\n\n安装：fnOS 应用中心手动安装 fpk（数据目录 /var/apps/fnmusic-flow-community/...，升级重装数据保留）。\n详见 README。")
        print("3) release:", out)

    # 4) 验证
    print("4) repo url: https://github.com/%s/%s" % (USER, REPO))
    print("   release url: https://github.com/%s/%s/releases/tag/v1.3.3" % (USER, REPO))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
